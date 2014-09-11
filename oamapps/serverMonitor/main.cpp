/***************************************************************************
 * $Id: main.cpp 34 2006-09-29 21:13:54Z dhill $
 *
 *   Copyright (C) 2006 Calpont Corporation
 *   All rights reserved
 *   Author: David Hill
 ***************************************************************************/

#include "serverMonitor.h"

using namespace std;
using namespace servermonitor;
using namespace oam;
using namespace logging;


/*****************************************************************************
* @brief	main
*
* purpose:	Launch Resource Monitor threads and call Hardware Monitor function
*
*
******************************************************************************/

int main (int argc, char** argv)
{
	ServerMonitor serverMonitor;
	Oam oam;

	try {
		oam.processInitComplete("ServerMonitor");
		LoggingID lid(SERVER_MONITOR_LOG_ID);
		MessageLog ml(lid);
		Message msg;
		Message::Args args;
		args.add("processInitComplete Successfully Called");
		msg.format(args);
		ml.logInfoMessage(msg);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		LoggingID lid(SERVER_MONITOR_LOG_ID);
		MessageLog ml(lid);
		Message msg;
		Message::Args args;
		args.add("EXCEPTION ERROR on processInitComplete: ");
		args.add(error);
		msg.format(args);
		ml.logErrorMessage(msg);
	}
	catch(...)
	{
		LoggingID lid(SERVER_MONITOR_LOG_ID);
		MessageLog ml(lid);
		Message msg;
		Message::Args args;
		args.add("EXCEPTION ERROR on processInitComplete: Caught unknown exception!");
		msg.format(args);
		ml.logErrorMessage(msg);
	}

	//wait until system is active before launching monitoring threads
	while(true)
	{
		SystemStatus systemstatus;
		try {
			oam.getSystemStatus(systemstatus);
		}
		catch (exception& ex)
		{}
		
		if (systemstatus.SystemOpState == oam::ACTIVE ) {
			//Launch CPU Monitor Thread
			pthread_t cpuMonitorThread;
			pthread_create (&cpuMonitorThread, NULL, (void*(*)(void*)) &cpuMonitor, NULL);
		
			//Launch Memory Monitor Thread
			pthread_t memoryMonitorThread;
			pthread_create (&memoryMonitorThread, NULL, (void*(*)(void*)) &memoryMonitor, NULL);
		
			//Launch Disk Monitor Thread
			pthread_t diskMonitorThread;
			pthread_create (&diskMonitorThread, NULL, (void*(*)(void*)) &diskMonitor, NULL);

			//Launch DB Health Check Thread
			pthread_t dbhealthMonitorThread;
			pthread_create (&dbhealthMonitorThread, NULL, (void*(*)(void*)) &dbhealthMonitor, NULL);

			//Call msg process request function
			msgProcessor();
		}
		sleep(5);
	}

	return 0;
}
