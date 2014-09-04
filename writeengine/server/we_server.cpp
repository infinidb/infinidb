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

/*******************************************************************************
* $Id: we_server.cpp 4702 2013-07-08 20:06:14Z bpaul $
*
*******************************************************************************/

#include <unistd.h>
#include <iostream>
#include <string>
#include <sstream>
#ifndef _MSC_VER
#include <signal.h>
#include <stdexcept>
#include "logger.h"
#endif
#ifndef _MSC_VER
#include <sys/resource.h>
#endif
using namespace std;

#include "messagequeue.h"
using namespace messageqcpp;

#include "threadpool.h"
using namespace threadpool;

#include "we_readthread.h"
using namespace WriteEngine;

#include "liboamcpp.h"
using namespace oam;

#include "distributedenginecomm.h"

#include "utils_utf8.h"

namespace
{
	void added_a_pm(int)
	{
		logging::LoggingID logid(21, 0, 0);
		logging::Message::Args args1;
		logging::Message msg(1);
		args1.add("we_server caught SIGHUP. Resetting connections");
		msg.format( args1 );
		logging::Logger logger(logid.fSubsysID);
		logger.logMessage(logging::LOG_TYPE_DEBUG, msg, logid);
		joblist::DistributedEngineComm::reset();
	}
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

int main(int argc, char** argv)
{
	// get and set locale language
    string systemLang = "C";
	systemLang = funcexp::utf8::idb_setlocale();

    printf ("Locale is : %s\n", systemLang.c_str() );

	//set BUSY_INIT state
	{
		// Is there a reason to have a seperate Oam instance for this?
		Oam oam;
		try
		{
			oam.processInitComplete("WriteEngineServer", oam::BUSY_INIT);
		}
		catch (...)
		{
		}
	}
	//BUG 2991
	setlocale(LC_NUMERIC, "C");
#ifndef _MSC_VER
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
    sa.sa_handler = added_a_pm;
	sigaction(SIGHUP, &sa, 0);
	sa.sa_handler = SIG_IGN;
	sigaction(SIGPIPE, &sa, 0);
#endif

	// Init WriteEngine Wrapper (including Config Calpont.xml cache)
	WriteEngine::WriteEngineWrapper::init( WriteEngine::SUBSYSTEM_ID_WE_SRV );

	Config weConfig;
	int rc;
	rc = setupResources();
	
	ostringstream serverParms;
	serverParms << "pm" << weConfig.getLocalModuleID() << "_WriteEngineServer";

	// Create MessageQueueServer, with one retry in case the call to bind the
	// known port fails with "Address already in use".
	boost::scoped_ptr<MessageQueueServer> mqs;
	bool tellUser = true;
	for (;;)
	{
		try {
			mqs.reset(new MessageQueueServer(serverParms.str()));
			break;
		}
		// @bug4393 Error Handling for MessageQueueServer constructor exception
		catch (runtime_error& re) {
			string what = re.what();
			if (what.find("Address already in use") != string::npos)
			{
				if (tellUser)
				{
					cerr << "Address already in use, retrying..." << endl;
					tellUser = false;
				}
				sleep(5);
			}
			else
			{
				Oam oam;
				try // Get out of BUSYINIT state; else OAM will not retry
				{
					oam.processInitComplete("WriteEngineServer");
				}
				catch (...)
				{
				}

				// If/when a common logging class or function is added to the
				// WriteEngineServer, we should use that.  In the mean time,
				// I will log this errmsg with inline calls to the logging.
				logging::Message::Args args;
				logging::Message message;
				string errMsg("WriteEngineServer failed to initiate: ");
				errMsg += what;
				args.add( errMsg );
				message.format(args);
				logging::LoggingID lid(SUBSYSTEM_ID_WE_SRV);
				logging::MessageLog ml(lid);
				ml.logCriticalMessage( message );

				return 2;
			}
		}
	}

	IOSocket ios;
	size_t mt = 20;
	size_t qs = mt * 100;
	ThreadPool tp(mt, qs);

	//set ACTIVE state
	{
		Oam oam;
		try
		{
			oam.processInitComplete("WriteEngineServer", ACTIVE);
		}
		catch (...)
		{
		}
	}
	cout << "WriteEngineServer is ready" << endl;
	for (;;)
	{
		try // BUG 4834 -
		{
			ios = mqs->accept();
			//tp.invoke(ReadThread(ios));
			ReadThreadFactory::CreateReadThread(tp,ios);
			{
				logging::Message::Args args;
				logging::Message message;
				string aMsg("WriteEngineServer : New incoming connection");
				args.add(aMsg);
				message.format(args);
				logging::LoggingID lid(SUBSYSTEM_ID_WE_SRV);
				logging::MessageLog ml(lid);
				ml.logInfoMessage( message );
			}
		}
		catch(std::exception& ex) // BUG 4834 - log the exception
		{
			logging::Message::Args args;
			logging::Message message;
			string errMsg("WriteEngineServer : Exception caught on accept(): ");
			errMsg += ex.what();
			args.add( errMsg );
			message.format(args);
			logging::LoggingID lid(SUBSYSTEM_ID_WE_SRV);
			logging::MessageLog ml(lid);
			ml.logCriticalMessage( message );
			break;
		}
	}

	//It is an error to reach here...
	return 1;
}

