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

/***************************************************************************
 * $Id: dbhealthMonitor.cpp 34 2006-09-29 21:13:54Z dhill $
 *
 *   Author: Zhixuan Zhu
 ***************************************************************************/

#include "serverMonitor.h"
#include "installdir.h"

using namespace std;
using namespace oam;
using namespace snmpmanager;
using namespace logging;
using namespace servermonitor;
using namespace config;


/*****************************************************************************************
* @brief	dbhealthMonitor Thread
*
* purpose:	check db health
*
*****************************************************************************************/
void dbhealthMonitor()
{
	ServerMonitor serverMonitor;
	Oam oam;

	oamModuleInfo_t t;

	//get local module info
	string localModuleName;
	int serverInstallType = 2;
	string OAMParentModuleName;

	try {
		t = oam.getModuleInfo();
		OAMParentModuleName = boost::get<3>(t);
	}
	catch (...) {}

	//Wait until DMLProc is Active, don't want to run if in rollback mode
	while(true)
	{
		try{
			ProcessStatus procstat;
			oam.getProcessStatus("DMLProc", OAMParentModuleName, procstat);
			if ( procstat.ProcessOpState == oam::ACTIVE)
				break;
		}
		catch(...)
		{}
		sleep(10);
	}

	bool setlog = false;
	bool clearlog = false;
	while(true)
	{
		try {
			t = oam.getModuleInfo();
			localModuleName = boost::get<0>(t);
			OAMParentModuleName = boost::get<3>(t);
			serverInstallType = boost::get<5>(t);
		}
		catch (...) {}

		string DBFunctionalMonitorFlag;
		try {
			oam.getSystemConfig( "DBFunctionalMonitorFlag", DBFunctionalMonitorFlag);
		}
		catch(...) {}

		// run on um1 or active pm
		if ( localModuleName == "um1" || 
			( localModuleName == OAMParentModuleName && 
			serverInstallType == oam::INSTALL_COMBINE_DM_UM_PM ) ) {

			if (DBFunctionalMonitorFlag == "y" ) {
				if (!setlog ) {
					try {
						LoggingID lid(SERVER_MONITOR_LOG_ID);
						MessageLog ml(lid);
						Message msg;
						Message::Args args;
						args.add("DBFunctionalMonitorFlag set: Running dbfunctional tester");
						msg.format(args);
						ml.logDebugMessage(msg);
					}
					catch (...)
					{}
					setlog = true;
				}
	
				serverMonitor.healthCheck();
			}
			else
			{
				if (!clearlog ) {
					try {
						LoggingID lid(SERVER_MONITOR_LOG_ID);
						MessageLog ml(lid);
						Message msg;
						Message::Args args;
						args.add("DBFunctionalMonitorFlag not-set: Not Running dbfunctional tester");
						msg.format(args);
						ml.logDebugMessage(msg);
					}
					catch (...)
					{}
					clearlog = true;
				}
			}
		}
		// sleep
		sleep(MONITOR_PERIOD);

	} // end of while loop
}

pthread_mutex_t FUNCTION_LOCK;

int ServerMonitor::healthCheck(bool action)
{
	Oam oam;

	pthread_mutex_lock(&FUNCTION_LOCK);

	//get local module name
	string localModuleName;
	oamModuleInfo_t t;
	try {
		t = oam.getModuleInfo();
		localModuleName = boost::get<0>(t);
	}
	catch (...) {}

	//get action 
	string DBHealthMonitorAction;
	oam.getSystemConfig( "DBHealthMonitorAction", DBHealthMonitorAction);

	GRACEFUL_FLAG gracefulTemp = GRACEFUL;
	ACK_FLAG ackTemp = ACK_YES;
			
	//run Health script
	string cmd = startup::StartUp::installDir() + "/bin/dbhealth.sh > /var/log/Calpont/dbfunctional.log1 2>&1";
	system(cmd.c_str());

	if (!oam.checkLogStatus("/var/log/Calpont/dbfunctional.log1", "OK")) {
		if (oam.checkLogStatus("/var/log/Calpont/dbfunctional.log1", "ERROR 1045") ) {
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("dbhealth.sh: Missing Password error");
			msg.format(args);
			ml.logDebugMessage(msg);
		}
		else
		{
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("DB Functional check failed");
			msg.format(args);
			ml.logCriticalMessage(msg);

			if (action) {
				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("Send Notification for DB Functional check failed and perform OAM Command");
				args.add( DBHealthMonitorAction);
				msg.format(args);
				ml.logDebugMessage(msg);

				oam.sendDeviceNotification(localModuleName, DB_HEALTH_CHECK_FAILED);

				if ( DBHealthMonitorAction == "stopSystem") {
					try
					{
						oam.stopSystem(gracefulTemp, ackTemp);
					}
					catch (...)
					{
					}
				}
				else if ( DBHealthMonitorAction == "restartSystem") {
					try
					{
						oam.restartSystem(gracefulTemp, ackTemp);
					}
					catch (...)
					{
					}
				}
				else if ( DBHealthMonitorAction == "shutdownSystem") {
					try
					{
						oam.shutdownSystem(gracefulTemp, ackTemp);
					}
					catch (...)
					{
					}
				}
			}
		}
		pthread_mutex_unlock(&FUNCTION_LOCK);

		return API_FAILURE;
	}
	pthread_mutex_unlock(&FUNCTION_LOCK);
	return API_SUCCESS;
}


