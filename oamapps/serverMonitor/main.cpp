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

#include "IDBPolicy.h"
#include "serverMonitor.h"

using namespace std;
using namespace servermonitor;
using namespace oam;
using namespace logging;

extern int swapFlag;

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

	//Launch Memory Monitor Thread and check if swap is in critical condition
	pthread_t memoryMonitorThread;
	pthread_create (&memoryMonitorThread, NULL, (void*(*)(void*)) &memoryMonitor, NULL);


	// initialize IDBPolicy while waiting swap flag being set.
	idbdatafile::IDBPolicy::configIDBPolicy();

	// wait until swap flag is set.
	while ( swapFlag == 0 )
	{
		sleep(1);
	}

	if ( swapFlag == 1 )
	{
		try {
			oam.processInitFailure();
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("processInitFailure Called");
			msg.format(args);
			ml.logInfoMessage(msg);
			sleep(5);
			exit(1);
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
			sleep(5);
			exit(1);
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
			sleep(5);
			exit(1);
		}
	}
	else
	{
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
	}

	//Ignore SIGPIPE signals
	signal(SIGPIPE, SIG_IGN);

	//Ignore SIGHUP signals
	signal(SIGHUP, SIG_IGN);

	//get auto rsync setting
	string umAutoSync = "n";	// default to 'n'
	try {
		oam.getSystemConfig( "UMAutoSync", umAutoSync);
	}
	catch(...) {
		umAutoSync = "n";
	}

	oamModuleInfo_t t;

	//get local module info
	string localModuleName;
	string localModuleType;
	int serverInstallType = 2;
	string OAMParentModuleName;

	try {
		t = oam.getModuleInfo();
		localModuleName = boost::get<0>(t);
		localModuleType = boost::get<1>(t);
		serverInstallType = boost::get<5>(t);
	}
	catch (...) {}
	
	string SingleServerInstall = "n";	// default to 'n'
	try {
		oam.getSystemConfig( "SingleServerInstall", SingleServerInstall);
	}
	catch(...) {
		SingleServerInstall = "n";
	}

	//Launch Rsync Thread, if needed
	// run on first non-disabled user-module
	// if combo um/pm configured a non single-server, then that is a pm
	// if separate um / pm, then that is a um
	bool launchUMAutoSync = false;
	SystemStatus systemstatus;
	if (umAutoSync == "y" )
	{
		if ( serverInstallType == oam::INSTALL_COMBINE_DM_UM_PM )
		{
			if ( SingleServerInstall != "y" )
			{	//get first non-disabled pm
				try
				{
					oam.getSystemStatus(systemstatus);
			
					for( unsigned int i = 0 ; i < systemstatus.systemmodulestatus.modulestatus.size(); i++)
					{
						if( systemstatus.systemmodulestatus.modulestatus[i].Module.empty() )
							// end of list
							break;
			
						string moduleName =  systemstatus.systemmodulestatus.modulestatus[i].Module;
						string moduleType = moduleName.substr(0,MAX_MODULE_TYPE_SIZE);
						if ( moduleType == "pm" ) {
							int state = systemstatus.systemmodulestatus.modulestatus[i].ModuleOpState;
							if ( state == oam::MAN_DISABLED || state == oam::AUTO_DISABLED )
								continue;
							else {
								//module is enabled, runs if this is pm1 and only pm1, so it will not run
								//if pm1 is down for an extented period of time
								if ( moduleName == "pm1" )
								{
									if (localModuleName == "pm1" )
										launchUMAutoSync = true;
									break;
								}
							}
						}
					}
				}
				catch(...) {}
			}
		}
		else
		{	//get first non-disabled um
			if ( localModuleType == "um" )
			{
				try
				{
					oam.getSystemStatus(systemstatus);
			
					for( unsigned int i = 0 ; i < systemstatus.systemmodulestatus.modulestatus.size(); i++)
					{
						if( systemstatus.systemmodulestatus.modulestatus[i].Module.empty() )
							// end of list
							break;
			
						string moduleName =  systemstatus.systemmodulestatus.modulestatus[i].Module;
						string moduleType = moduleName.substr(0,MAX_MODULE_TYPE_SIZE);
						if ( moduleType == "um" ) {
							int state = systemstatus.systemmodulestatus.modulestatus[i].ModuleOpState;
							if ( state == oam::MAN_DISABLED || state == oam::AUTO_DISABLED )
								continue;
							else {
								//module is enabled, runs if this is um1 and only um1, so it will not run
								//if um1 is down for an extented period of time
								if ( moduleName == "um1" )
								{
									if (localModuleName == "um1" )
										launchUMAutoSync = true;
									break;
								}
							}	
						}
					}
				}
				catch(...) {}
			}
		}
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

			if (launchUMAutoSync) {
				//Launch UM Auto Sync Thread
				pthread_t rsyncThread;
				pthread_create (&rsyncThread, NULL, (void*(*)(void*)) &UMAutoSync, NULL);
			}

			//Launch CPU Monitor Thread
			pthread_t cpuMonitorThread;
			pthread_create (&cpuMonitorThread, NULL, (void*(*)(void*)) &cpuMonitor, NULL);
		
			//Launch Disk Monitor Thread
			pthread_t diskMonitorThread;
			pthread_create (&diskMonitorThread, NULL, (void*(*)(void*)) &diskMonitor, NULL);
		
			//Launch DB Health Check Thread
//			pthread_t dbhealthMonitorThread;
//			pthread_create (&dbhealthMonitorThread, NULL, (void*(*)(void*)) &dbhealthMonitor, NULL);

			//Call msg process request function
			msgProcessor();
			
			break;
		}
		sleep(5);
	}

	return 0;
}

