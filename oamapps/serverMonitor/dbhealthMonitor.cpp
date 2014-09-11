/***************************************************************************
 * $Id: dbhealthMonitor.cpp 34 2006-09-29 21:13:54Z dhill $
 *
 *   Author: Zhixuan Zhu
 ***************************************************************************/

#include "serverMonitor.h"

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

	try {
		t = oam.getModuleInfo();
		localModuleName = boost::get<0>(t);
		OAMParentModuleName = boost::get<3>(t);
		serverInstallType = boost::get<5>(t);
	}
	catch (...) {}

	int DBHealthAutoRunTime = 0;
	
	// run on um1 or active pm
	if ( localModuleName == "um1" || 
		( localModuleName == OAMParentModuleName && 
		serverInstallType == oam::INSTALL_COMBINE_DM_UM_PM ) )
		DBHealthAutoRunTime = 0;
	else
		pthread_exit(0);

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

	//don't run if timer is set to 0
	DBHealthAutoRunTime = 0;		
	try {
		oam.getSystemConfig ("DBHealthAutoRunTime", DBHealthAutoRunTime);
	} catch (...)
	{
		DBHealthAutoRunTime = 0;
	}

	if ( DBHealthAutoRunTime == 0 )
		pthread_exit(0);

	LoggingID lid(SERVER_MONITOR_LOG_ID);
	MessageLog ml(lid);
	Message msg;
	Message::Args args;
	args.add("DBHealthAutoRunTime set: Running dbhealth tester");
	msg.format(args);
	ml.logDebugMessage(msg);

	while(true)
	{
		//don't run if timer is set to 0
		DBHealthAutoRunTime = 0;		
		try {
			oam.getSystemConfig ("DBHealthAutoRunTime", DBHealthAutoRunTime);
		} catch (...)
		{
			DBHealthAutoRunTime = 0;
		}
	
		if ( DBHealthAutoRunTime != 0 ) {
			//skip running if system or local module is not ACTIVE
			SystemStatus systemstatus;
			try {
				oam.getSystemStatus(systemstatus);
			}
			catch (exception& ex)
			{}
			
			if (systemstatus.SystemOpState == oam::ACTIVE ) {
				try{
					int opState;
					bool degraded;
					oam.getModuleStatus(localModuleName, opState, degraded);
			
					if (opState == oam::ACTIVE )
						serverMonitor.healthCheck();
				}
				catch (exception& ex)
				{}
			}	
		}
		else
			DBHealthAutoRunTime = 60;

		// if set to a negive number, exit after 1 run
		if ( DBHealthAutoRunTime < 0 ) {
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("DBHealthAutoRunTime set to a negive number: exiting after 1 run");
			msg.format(args);
			ml.logDebugMessage(msg);
			pthread_exit(0);
		}

		// sleep
		sleep(DBHealthAutoRunTime);

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
	string DBHealthMonitorAction = "none";
	try {
		oam.getSystemConfig( "DBHealthMonitorAction", DBHealthMonitorAction);
	}
	catch (...) {
		DBHealthMonitorAction = "none";
	}

	GRACEFUL_FLAG gracefulTemp = GRACEFUL;
	ACK_FLAG ackTemp = ACK_YES;
			
	//run Health script
	string cmd = "/usr/local/Calpont/bin/dbhealth.sh > /var/log/Calpont/dbhealth.log1 2>&1";
	system(cmd.c_str());

	if (!oam.checkLogStatus("/var/log/Calpont/dbhealth.log1", "OK")) {
		if (oam.checkLogStatus("/var/log/Calpont/dbhealth.log1", "ERROR 1045") ) {
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
			args.add("DB Health check failed");
			msg.format(args);
			ml.logCriticalMessage(msg);

			if (action) {
				//only take action is system is ACTIVE
				SystemStatus systemstatus;
				try {
					oam.getSystemStatus(systemstatus);
				}
				catch (exception& ex)
				{}
				
				if (systemstatus.SystemOpState == oam::ACTIVE ) {
					LoggingID lid(SERVER_MONITOR_LOG_ID);
					MessageLog ml(lid);
					Message msg;
					Message::Args args;
					args.add("Send Notification for DB Health check failed and perform OAM Command");
					args.add( DBHealthMonitorAction);
					msg.format(args);
					ml.logCriticalMessage(msg);
	
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
				else
				{
					LoggingID lid(SERVER_MONITOR_LOG_ID);
					MessageLog ml(lid);
					Message msg;
					Message::Args args;
					args.add("System not in Active State, take no action on the DB Health check failure");
					args.add( DBHealthMonitorAction);
					msg.format(args);
					ml.logInfoMessage(msg);
				}
			}
		}

		pthread_mutex_unlock(&FUNCTION_LOCK);

		return API_FAILURE;
	}
	pthread_mutex_unlock(&FUNCTION_LOCK);
	return API_SUCCESS;
}


