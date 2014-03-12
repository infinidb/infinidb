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

/***************************************************************************
* $Id: processmonitor.cpp 2044 2013-08-07 19:47:37Z dhill $
*
 ***************************************************************************/

#include <boost/scoped_ptr.hpp>

#include "IDBDataFile.h"
#include "IDBPolicy.h"
#include "processmonitor.h"
#include "installdir.h"
#include "cacheutils.h"
#include "ddlcleanuputil.h"
using namespace cacheutils;

using namespace std;
using namespace oam;
using namespace messageqcpp;
using namespace snmpmanager;
using namespace logging;
using namespace config;

using namespace idbdatafile;

extern string systemOAM;
extern string dm_server;
extern bool runStandby;
extern bool processInitComplete;
extern int fmoduleNumber;
extern string cloud;
extern string GlusterConfig;
extern bool rootUser;
extern string USER;
extern bool HDFS;
extern string PMwithUM;

//std::string gOAMParentModuleName;
bool gOAMParentModuleFlag;
bool gOAMStandbyModuleFlag;

typedef boost::tuple<std::string, ALARMS, int> sendAlarmInfo_t;
typedef boost::tuple<std::string, int, pid_t> sendProcessInfo_t;

pthread_mutex_t ALARM_LOCK;
pthread_mutex_t LIST_LOCK;
pthread_mutex_t PROCESS_LOCK;

namespace processmonitor {

void sendAlarmThread (sendAlarmInfo_t* t);
void sendProcessThread (sendProcessInfo_t* t);

using namespace oam;

/******************************************************************************************
* @brief	MonitorConfig
*
* purpose:	MonitorConfig constructor
*
******************************************************************************************/

MonitorConfig::MonitorConfig()
{
	Oam oam;
	oamModuleInfo_t t;

	//get local module info
	try {
		t = oam.getModuleInfo();
		flocalModuleName = boost::get<0>(t);
		flocalModuleType = boost::get<1>(t);
		flocalModuleID = boost::get<2>(t);
		fOAMParentModuleName = boost::get<3>(t);
		fOAMParentModuleFlag = boost::get<4>(t);
		fserverInstallType = boost::get<5>(t);
		fOAMStandbyModuleName = boost::get<6>(t);
		fOAMStandbyModuleFlag = boost::get<7>(t);

		gOAMStandbyModuleFlag = boost::get<7>(t);
		gOAMParentModuleFlag = boost::get<4>(t);
	}
	catch (exception& e) {
		cout << endl << "ProcMon Construct Error reading getModuleInfo = " << e.what() << endl;
	}

//	cout << "OAMParentModuleName = " << fOAMParentModuleName << endl;

//	if ( fOAMParentModuleName == oam::UnassignedName ) {
//		cout << endl << "OAMParentModuleName == oam::UnassignedName, exiting " << endl;
//		exit (-1);
//	}

	//get calpont software version and release
	SystemSoftware systemsoftware;

	try
	{
		oam.getSystemSoftware(systemsoftware);

		fsoftwareVersion = systemsoftware.Version;
		fsoftwareRelease = systemsoftware.Release;
	}
	catch (exception& e) {
		cout << endl << "ProcMon Construct Error reading getSystemSoftware = " << e.what() << endl;
	}

}


/******************************************************************************************
* @brief	MonitorConfig destructor
*
* purpose:	MonitorConfig destructor
*
******************************************************************************************/
MonitorConfig::~MonitorConfig()
{
}

/******************************************************************************************
* @brief	MonitorLog Constructor
*
* purpose:	Constructor:open the log file for writing
*
******************************************************************************************/
MonitorLog::MonitorLog()
{
}

/******************************************************************************************
* @brief	MonitorLog Destructor
*
* purpose:	Destructor:close the log file
*
******************************************************************************************/
MonitorLog::~MonitorLog()
{
}

/******************************************************************************************
* @brief	writeLog for string
*
* purpose:	write string message to the log file
*
******************************************************************************************/
void	MonitorLog::writeLog(const int lineNumber, const string logContent, const LOG_TYPE logType)
{
	//Log this event 
	LoggingID lid(18);
	MessageLog ml(lid);
	Message msg;
	Message::Args args;
	args.add(logContent);
	msg.format(args);

	switch(logType) {
		case LOG_TYPE_DEBUG:
			ml.logDebugMessage(msg);
			break;
		case LOG_TYPE_INFO:
			ml.logInfoMessage(msg);
			break;
		case LOG_TYPE_WARNING:
			ml.logWarningMessage(msg);
			break;
		case LOG_TYPE_ERROR:
			args.add("line:");
			args.add(lineNumber);
			ml.logErrorMessage(msg);
			break;
		case LOG_TYPE_CRITICAL:
			ml.logCriticalMessage(msg);
			break;
	}
	return;
}
 
/******************************************************************************************
* @brief	writeLog for integer
*
* purpose:	write integer information to the log file
*
******************************************************************************************/
void	MonitorLog::writeLog(const int lineNumber, const int logContent, const LOG_TYPE logType)
{
	//Log this event 
	LoggingID lid(18);
	MessageLog ml(lid);
	Message msg;
	Message::Args args;
	args.add(logContent);
	msg.format(args);

	switch(logType) {
		case LOG_TYPE_DEBUG:
			ml.logDebugMessage(msg);
			break;
		case LOG_TYPE_INFO:
			ml.logInfoMessage(msg);
			break;
		case LOG_TYPE_WARNING:
			ml.logWarningMessage(msg);
			break;
		case LOG_TYPE_ERROR:
			args.add("line:");
			args.add(lineNumber);
			ml.logErrorMessage(msg);
			break;
		case LOG_TYPE_CRITICAL:
			ml.logCriticalMessage(msg);
			break;
	}
	return;
}

/******************************************************************************************
* @brief	ProcessMonitor Constructor
*
* purpose:	ProcessMonitor Constructor
*
******************************************************************************************/
ProcessMonitor::ProcessMonitor(MonitorConfig &aconfig, MonitorLog &alog):
config(aconfig), log(alog)
{
//	log.writeLog(__LINE__, "Process Monitor starts");
}

/******************************************************************************************
* @brief	ProcessMonitor Default Destructor
*
* purpose:	ProcessMonitor Default Destructor
*
******************************************************************************************/
ProcessMonitor::~ProcessMonitor()
{
}

/******************************************************************************************
* @brief	statusListPtr
*
* purpose:	return the process status list
*
******************************************************************************************/
processStatusList*	ProcessMonitor::statusListPtr()
{
	return &fstatusListPtr;
}

/******************************************************************************************
* @brief	buildList
*
* purpose:	Build a list of processes the monitor started
*
******************************************************************************************/
void  MonitorConfig::buildList(string ProcessModuleType, string processName, string ProcessLocation, 
		string arg_list[MAXARGUMENTS], uint16_t launchID, pid_t processID, 
		uint16_t state, uint16_t BootLaunch, string RunType, 
		string DepProcessName[MAXDEPENDANCY], string DepModuleName[MAXDEPENDANCY],
		string LogFile)
{
	//check if the process is already in the list 
	MonitorLog log;
	Oam oam;

	if ( processName == "mysqld" )
		return;

	pthread_mutex_lock(&LIST_LOCK);

	// get current time in seconds
	time_t cal;
	time (&cal);

	processList::iterator listPtr;
	processList* aPtr = monitoredListPtr();

	//Log the current list 
/*	log.writeLog(__LINE__, "");
	log.writeLog(__LINE__, "BEGIN: The current list in this monitor is");

	for (listPtr=aPtr->begin(); listPtr != aPtr->end(); ++listPtr)
	{
		log.writeLog(__LINE__, (*listPtr).ProcessModuleType);
		log.writeLog(__LINE__, (*listPtr).ProcessName);
		log.writeLog(__LINE__, (*listPtr).ProcessLocation);
		log.writeLog(__LINE__, (*listPtr).currentTime);
		log.writeLog(__LINE__, (*listPtr).processID);
		log.writeLog(__LINE__, (*listPtr).state);
	}
*/

	listPtr = aPtr->begin();
	for (; listPtr != aPtr->end(); ++listPtr)
	{
		if ((*listPtr).ProcessName == processName)
			break;
	}

	if (listPtr == aPtr->end())
	{	// not in list, add it
		processInfo proInfo;
		proInfo.ProcessModuleType = ProcessModuleType;
		proInfo.ProcessName = processName;
		proInfo.ProcessLocation = ProcessLocation;

		for (unsigned int i=0; i < MAXARGUMENTS; i++)
		{
			if (arg_list[i].length() == 0)
				break;
			proInfo.ProcessArgs[i] = arg_list[i];
		}

		proInfo.launchID = launchID;
		proInfo.currentTime = cal;
		proInfo.processID = processID;
		proInfo.state = state;
		proInfo.BootLaunch = BootLaunch;
		proInfo.RunType = RunType;
		proInfo.LogFile = LogFile;
		proInfo.dieCounter = 0;

		for (unsigned int i=0; i < MAXDEPENDANCY; i++)
		{
			if (DepProcessName[i].length() == 0)
				break;
			proInfo.DepProcessName[i] = DepProcessName[i];
		}

		for (unsigned int i=0; i < MAXDEPENDANCY; i++)
		{
			if (DepModuleName[i].length() == 0)
				break;
			proInfo.DepModuleName[i] = DepModuleName[i];
		}

		listPtr = aPtr->begin();
		if ( listPtr == aPtr->end()) {
			// list empty, add first one
			fmonitoredListPtr.push_back(proInfo);
		}
		else
		{
			for (; listPtr != aPtr->end(); ++listPtr)
			{
				if ((*listPtr).launchID > launchID) {
					fmonitoredListPtr.insert(listPtr, proInfo);
					break;
				}
			}
			if ( listPtr == aPtr->end())
				fmonitoredListPtr.push_back(proInfo);
		}
	}
	else
	{
		// in list, just update the information

		if ( ProcessLocation.empty() )
			//status update only
			(*listPtr).state = state;
		else
		{
			(*listPtr).processID = processID;
			(*listPtr).currentTime = cal;
			(*listPtr).state = state;
			(*listPtr).launchID = launchID;
			(*listPtr).BootLaunch = BootLaunch;
			(*listPtr).RunType = RunType;
			(*listPtr).LogFile = LogFile;

			for (unsigned int i=0; i < MAXARGUMENTS; i++)
			{
				(*listPtr).ProcessArgs[i] = arg_list[i];
			}
		}
	}

	//Log the current list 
/*	log.writeLog(__LINE__, "");
	log.writeLog(__LINE__, "END: The current list in this monitor is");

	for (listPtr=aPtr->begin(); listPtr != aPtr->end(); ++listPtr)
	{
		log.writeLog(__LINE__, (*listPtr).ProcessModuleType);
		log.writeLog(__LINE__, (*listPtr).ProcessName);
		log.writeLog(__LINE__, (*listPtr).ProcessLocation);
		log.writeLog(__LINE__, (*listPtr).currentTime);
		log.writeLog(__LINE__, (*listPtr).processID);
		log.writeLog(__LINE__, (*listPtr).state);
	}
*/
	pthread_mutex_unlock(&LIST_LOCK);

	return;
}

/******************************************************************************************
* @brief	monitoredListPtr
*
* purpose:	return the process list
*
******************************************************************************************/
processList*	MonitorConfig::monitoredListPtr()
{
	return &fmonitoredListPtr;
}

/******************************************************************************************
* @brief	processMessage
*
* purpose:	receive and process message
*
******************************************************************************************/
void ProcessMonitor::processMessage(messageqcpp::ByteStream msg, messageqcpp::IOSocket mq)

{
	Oam oam;
	ByteStream	ackMsg;
	
	ByteStream::byte messageType;
	ByteStream::byte requestID;
	ByteStream::byte actIndicator;
	ByteStream::byte manualFlag;
	string processName;

	msg >> messageType;

	switch (messageType) {
		case REQUEST:
			{
			msg >> requestID;
			msg >> actIndicator;

			if (!processInitComplete) {
				ackMsg << (ByteStream::byte) ACK;
				ackMsg << (ByteStream::byte) requestID;
				ackMsg << (ByteStream::byte) oam::API_FAILURE;
				mq.write(ackMsg);
				break;
			}

			switch (requestID) {
				case STOP:
				{
					msg >> processName;
					msg >> manualFlag;
					log.writeLog(__LINE__,  "MSG RECEIVED: Stop process request on " + processName);
					int requestStatus = API_SUCCESS;
		
					processList::iterator listPtr;
					processList* aPtr = config.monitoredListPtr();
					listPtr = aPtr->begin();

					for (; listPtr != aPtr->end(); ++listPtr)
					{
						if ((*listPtr).ProcessName == processName) {
							// update local process state
							if ( manualFlag ) {
								(*listPtr).state = oam::MAN_OFFLINE;
								(*listPtr).dieCounter = 0;
							}
							else
								(*listPtr).state = oam::AUTO_OFFLINE;
							
							//stop the process first 
							if (stopProcess((*listPtr).processID, (*listPtr).ProcessName, (*listPtr).ProcessLocation, actIndicator, manualFlag))
								requestStatus = API_FAILURE;
							else
								(*listPtr).processID = 0;
							break;
						}
					}
		
					if (listPtr == aPtr->end())
					{
						log.writeLog(__LINE__,  "ERROR: No such process: " + processName);
						requestStatus = API_FAILURE;
					}

					ackMsg << (ByteStream::byte) ACK;
					ackMsg << (ByteStream::byte) STOP;
					ackMsg << (ByteStream::byte) requestStatus;
					mq.write(ackMsg);

					log.writeLog(__LINE__, "STOP: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

					break;
				}
				case START:
				{
					msg >> processName; 
					msg >> manualFlag;
					log.writeLog(__LINE__, "MSG RECEIVED: Start process request on: " + processName);
		
					ProcessConfig processconfig;
					ProcessStatus processstatus;
					try {
						//Get the process information 
						Oam oam;
						oam.getProcessConfig(processName, config.moduleName(), processconfig);
			
						oam.getProcessStatus(processName, config.moduleName(), processstatus);
					}
					catch (exception& ex)
					{
						string error = ex.what();
						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: " + error, LOG_TYPE_ERROR );
					}
					catch(...)
					{
						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: Caught unknown exception!", LOG_TYPE_ERROR );
					}
		
					int requestStatus = API_SUCCESS;

					//check the process current status & start the requested process
					if (processstatus.ProcessOpState != oam::ACTIVE) {

						int initType = oam::STANDBY_INIT;
						if ( actIndicator == oam::GRACEFUL_STANDBY) {
							//this module running Parent OAM Standby
							runStandby = true;
							log.writeLog(__LINE__,  "ProcMon Running Hot-Standby");

							// delete any old active alarm log file
							unlink ("/var/log/Calpont/activeAlarms");
						}

						//Check for SIMPLEX runtype processes
						initType = checkSpecialProcessState( processconfig.ProcessName, processconfig.RunType, processconfig.ModuleType );

						if ( initType == oam::COLD_STANDBY) {
							//there is a mate active, skip
							config.buildList(processconfig.ModuleType,
												processconfig.ProcessName,
												processconfig.ProcessLocation, 
												processconfig.ProcessArgs, 
												processconfig.LaunchID,
												0, 
												oam::COLD_STANDBY, 
												processconfig.BootLaunch,
												processconfig.RunType,
												processconfig.DepProcessName, 
												processconfig.DepModuleName,
												processconfig.LogFile);
	
							requestStatus = API_SUCCESS;
							ackMsg << (ByteStream::byte) ACK;
							ackMsg << (ByteStream::byte) START;
							ackMsg << (ByteStream::byte) requestStatus;
							mq.write(ackMsg);
							//sleep(1);	

							log.writeLog(__LINE__, "START: process left STANDBY " + processName);
							log.writeLog(__LINE__, "START: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));
							break;
						}

						pid_t processID = startProcess(processconfig.ModuleType,
										processconfig.ProcessName,
										processconfig.ProcessLocation, 
										processconfig.ProcessArgs, 
										processconfig.LaunchID,
										processconfig.BootLaunch,
										processconfig.RunType,
										processconfig.DepProcessName, 
										processconfig.DepModuleName,
										processconfig.LogFile,
										initType,
										actIndicator);
						if ( processID > oam::API_MAX )
							processID = oam::API_SUCCESS;
						requestStatus = processID;
					}
					else
						log.writeLog(__LINE__, "START: process already active " + processName);

					//Inform Process Manager that Process restart
					processRestarted(processName);

					ackMsg << (ByteStream::byte) ACK;
					ackMsg << (ByteStream::byte) START;
					ackMsg << (ByteStream::byte) requestStatus;
					mq.write(ackMsg);
	
					log.writeLog(__LINE__, "START: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

					break;
				}
				case RESTART:
				{
					msg >> processName;
					msg >> manualFlag;
					log.writeLog(__LINE__,  "MSG RECEIVED: Restart process request on " + processName);
					int requestStatus = API_SUCCESS;

					// check for mysql restart
					if ( processName == "mysql" ) {
						try {
							oam.actionMysqlCalpont(MYSQL_RESTART);
						}
						catch(...)
						{}

						ackMsg << (ByteStream::byte) ACK;
						ackMsg << (ByteStream::byte) RESTART;
						ackMsg << (ByteStream::byte) API_SUCCESS;
						mq.write(ackMsg);
	
						log.writeLog(__LINE__, "RESTART: ACK back to ProcMgr, return status = " + oam.itoa((int) API_SUCCESS));
	
						break;
					}

					processList::iterator listPtr;
					processList* aPtr = config.monitoredListPtr();
					listPtr = aPtr->begin();
		
					for (; listPtr != aPtr->end(); ++listPtr)
					{
						if ((*listPtr).ProcessName == processName) {
							// update local process state 
							if ( manualFlag ) {
								(*listPtr).state = oam::MAN_OFFLINE;
								(*listPtr).dieCounter = 0;
							}
							else
								(*listPtr).state = oam::AUTO_OFFLINE;

							//stop the process first 
							if (stopProcess((*listPtr).processID, (*listPtr).ProcessName, (*listPtr).ProcessLocation, actIndicator, manualFlag))
								requestStatus = API_FAILURE;
							else
							{
//								sleep(1);
								(*listPtr).processID = 0;

								//Check for SIMPLEX runtype processes
								int initType = checkSpecialProcessState( (*listPtr).ProcessName, (*listPtr).RunType, (*listPtr).ProcessModuleType );

								if ( initType == oam::COLD_STANDBY ) {
									//there is a mate active, skip
									(*listPtr).state = oam::COLD_STANDBY;
									requestStatus = API_SUCCESS;
									//sleep(1);
									break;
								}

								//start the process again
								pid_t processID = startProcess((*listPtr).ProcessModuleType,
																(*listPtr).ProcessName,
																(*listPtr).ProcessLocation, 
																(*listPtr).ProcessArgs, 
																(*listPtr).launchID,
																(*listPtr).BootLaunch,
																(*listPtr).RunType,
																(*listPtr).DepProcessName, 
																(*listPtr).DepModuleName,
																(*listPtr).LogFile,
																initType);

								if ( processID > oam::API_MAX )
									processID = oam::API_SUCCESS;
								requestStatus = processID;

							}
						break;
						}
					}
		
					if (listPtr == aPtr->end())
					{
						log.writeLog(__LINE__,  "ERROR: No such process: " + processName, LOG_TYPE_ERROR );
						requestStatus = API_FAILURE;
					}

					//Inform Process Manager that Process restart
					processRestarted(processName);

					ackMsg << (ByteStream::byte) ACK;
					ackMsg << (ByteStream::byte) RESTART;
					ackMsg << (ByteStream::byte) requestStatus;
					mq.write(ackMsg);

					log.writeLog(__LINE__, "RESTART: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

					break;
				}
				case PROCREINITPROCESS:
				{
					msg >> processName; 
					msg >> manualFlag;

					log.writeLog(__LINE__, "MSG RECEIVED: Re-Init process request on: " + processName);

					if ( processName == "cpimport" )
					{
						system("pkill -sighup cpimport");
					}
					else
					{
						processList::iterator listPtr;
						processList* aPtr = config.monitoredListPtr();
						listPtr = aPtr->begin();
			
						for (; listPtr != aPtr->end(); ++listPtr)
						{
							if ((*listPtr).ProcessName == processName) {
								if ( (*listPtr).processID <= 1 ) {
									log.writeLog(__LINE__,  "ERROR: process not active" , LOG_TYPE_DEBUG );
									break;
								}
	
								reinitProcess((*listPtr).processID, (*listPtr).ProcessName, actIndicator);
								break;
							}
						}
	
						if (listPtr == aPtr->end())
						{
							log.writeLog(__LINE__,  "ERROR: No such process: " + processName, LOG_TYPE_ERROR );
						}
					}

					log.writeLog(__LINE__, "PROCREINITPROCESS: completed, no ack to ProcMgr");
					break;
				}
				case STOPALL:
				{
					msg >> manualFlag;
					log.writeLog(__LINE__,  "MSG RECEIVED: Stop All process request...");

					if ( actIndicator == STATUS_UPDATE ) {
						//check and send notification
						MonitorConfig config;
						if ( config.moduleType() == "um" )
							oam.sendDeviceNotification(config.moduleName(), START_UM_DOWN);
						else
							if ( gOAMParentModuleFlag )
								oam.sendDeviceNotification(config.moduleName(), START_PM_MASTER_DOWN);
							else
								if (gOAMStandbyModuleFlag)
									oam.sendDeviceNotification(config.moduleName(), START_PM_STANDBY_DOWN);
								else
									oam.sendDeviceNotification(config.moduleName(), START_PM_COLD_DOWN);

						ackMsg << (ByteStream::byte) ACK;
						ackMsg << (ByteStream::byte) STOPALL;
						ackMsg << (ByteStream::byte) API_SUCCESS;
						mq.write(ackMsg);

						log.writeLog(__LINE__, "STOPALL: ACK back to ProcMgr, STATUS_UPDATE only performed");
						break;
					}

					//get local module run-type
					string runType = oam::LOADSHARE;	//default
					try{
						ModuleTypeConfig moduletypeconfig;
						oam.getSystemConfig(config.moduleType(), moduletypeconfig);
						runType = moduletypeconfig.RunType;
					}
					catch (exception& ex)
					{
						string error = ex.what();
						log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
					}
					catch(...)
					{
						log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
					}

					//Loop reversely through the process list, stop all processes 
					processList* aPtr = config.monitoredListPtr();
					processList::reverse_iterator rPtr;
					uint16_t rtnCode;
					int requestStatus = API_SUCCESS;

					for (rPtr = aPtr->rbegin(); rPtr != aPtr->rend(); ++rPtr)
					{
						if ( (*rPtr).BootLaunch == INIT_LAUNCH)
							//skip
							continue;

						if ( (*rPtr).BootLaunch == BOOT_LAUNCH && 
								gOAMParentModuleFlag )
							if ( actIndicator != INSTALL )
								//skip
								continue;

						// update local process state here so monitor thread doesn't jump on it
						if ( manualFlag ) {
							(*rPtr).state = oam::MAN_OFFLINE;
							(*rPtr).dieCounter = 0;
						}
						else
							(*rPtr).state = oam::AUTO_OFFLINE;
		
						rtnCode = stopProcess((*rPtr).processID, (*rPtr).ProcessName, (*rPtr).ProcessLocation, actIndicator, manualFlag);
						if (rtnCode)
							// error in stopping a process
							requestStatus = API_FAILURE;	
						else
							(*rPtr).processID = 0;
					}

					//reset BRM locks
					if ( requestStatus == oam::API_SUCCESS ) {
						string logdir("/var/log/Calpont");
						if (access(logdir.c_str(), W_OK) != 0) logdir = "/tmp";
						string cmd = startup::StartUp::installDir() + "/bin/reset_locks > " + logdir + "/reset_locks.log1 2>&1";
						system(cmd.c_str());
						log.writeLog(__LINE__, "BRM reset_locks script run", LOG_TYPE_DEBUG);

						//stop the mysql daemon
						try {
							oam.actionMysqlCalpont(MYSQL_STOP);
						}
						catch(...)
						{}

						//send down notification
						oam.sendDeviceNotification(config.moduleName(), MODULE_DOWN);

						//setModule status to offline
						if ( manualFlag ) {
							try{
								oam.setModuleStatus(config.moduleName(), oam::MAN_OFFLINE);
							}
							catch (exception& ex)
							{
								string error = ex.what();
								log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: " + error, LOG_TYPE_ERROR);
							}
							catch(...)
							{
								log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: Caught unknown exception!", LOG_TYPE_ERROR);
							}
						}
						else
						{
							try{
								oam.setModuleStatus(config.moduleName(), oam::AUTO_OFFLINE);
							}
							catch (exception& ex)
							{
								string error = ex.what();
								log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: " + error, LOG_TYPE_ERROR);
							}
							catch(...)
							{
								log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: Caught unknown exception!", LOG_TYPE_ERROR);
							}
						}
					}
					else
					{
						//setModule status to failed
						try{
							oam.setModuleStatus(config.moduleName(), oam::FAILED);
						}
						catch (exception& ex)
						{
							string error = ex.what();
							log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: " + error, LOG_TYPE_ERROR);
						}
						catch(...)
						{
							log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: Caught unknown exception!", LOG_TYPE_ERROR);
						}
					}


					if ( config.moduleType() == "pm" ) {
						//clearout auto move dbroots files
						string cmd = "rm -f " + startup::StartUp::installDir() + "/local/moveDbrootTransactionLog";
						system(cmd.c_str());
						cmd = "touch " + startup::StartUp::installDir() + "/local/moveDbrootTransactionLog";
						system(cmd.c_str());

						//go unmount disk NOT assigned to this pm
						unmountExtraDBroots();
					}

					ackMsg << (ByteStream::byte) ACK;
					ackMsg << (ByteStream::byte) STOPALL;
					ackMsg << (ByteStream::byte) requestStatus;
					mq.write(ackMsg);

					log.writeLog(__LINE__, "STOPALL: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

					//Remove Calpont RPM on REMOVE option
					if ( actIndicator == oam::REMOVE ) {
						log.writeLog(__LINE__,  "STOPALL: uninstall Calpont RPMs", LOG_TYPE_DEBUG);
						if ( config.moduleType() == "um" ) {
							system("rpm -e infinidb-libs infinidb-platform infinidb-enterprise infinidb-storage-engine infinidb-mysql --nodeps");
							system("dpkg -P infinidb-libs infinidb-platform infinidb-enterprise infinidb-storage-engine infinidb-mysql");
						}
						else	// pm
						{
							//Flush the cache
							cacheutils::flushPrimProcCache();
							cacheutils::dropPrimProcFdCache();
							flushInodeCache();

							string cmd = "umount " + startup::StartUp::installDir() + "/data* -l > /dev/null 2>&1";

							system(cmd.c_str());
							sleep(1);
	
							system("rpm -e infinidb-libs infinidb-platform infinidb-enterprise infinidb-storage-engine infinidb-mysql --nodeps");
							system("dpkg -P infinidb-libs infinidb-platform infinidb-enterprise infinidb-storage-engine infinidb-mysql");
						}
						// should get here is packages get removed correctly
						string cmd = startup::StartUp::installDir() + "/bin/infinidb stop > /dev/null 2>&1";
						system(cmd.c_str());
						exit (0);
					}

					break;
				}
				case STARTALL:
				{
					msg >> manualFlag;
					int requestStatus = oam::API_SUCCESS;
					log.writeLog(__LINE__,  "MSG RECEIVED: Start All process request...");

					//start the mysql daemon 
					try {
						oam.actionMysqlCalpont(MYSQL_START);
					}
					catch(...)
					{}

					if( config.moduleType() == "pm" )
					{
						//setup dbroot mounts
						createDataDirs(cloud);
						int ret = checkDataMount();
						if (ret != oam::API_SUCCESS)
						{
							int ret_status = oam::API_FAILURE;

							log.writeLog(__LINE__, "checkDataMount error, startmodule failed", LOG_TYPE_CRITICAL);
	
							ackMsg << (ByteStream::byte) ACK;
							ackMsg << (ByteStream::byte) STARTALL;
							ackMsg << (ByteStream::byte) ret_status;
							mq.write(ackMsg);
		
							log.writeLog(__LINE__, "STARTALL: ACK back to ProcMgr, return status = " + oam.itoa((int) oam::API_FAILURE));
			
							break;
						}
					}

					//run HDFS startup test script to perform basic DB sanity testing
					if ( HDFS ) {
						if ( runHDFSTest() != oam::API_SUCCESS ) {
							requestStatus = oam::API_FAILURE_DB_ERROR;

							ackMsg << (ByteStream::byte) ACK;
							ackMsg << (ByteStream::byte) STARTALL;
							ackMsg << (ByteStream::byte) requestStatus;
							mq.write(ackMsg);
		
							log.writeLog(__LINE__, "STARTALL: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));
			
							break;
						}
					}

					//Loop through all Process belong to this module	
					processList::iterator listPtr;
					processList* aPtr = config.monitoredListPtr();
					listPtr = aPtr->begin();
					requestStatus = API_SUCCESS;
	
					//launched any processes controlled by ProcMon that aren't active
					for (; listPtr != aPtr->end(); ++listPtr)
					{
						if ( (*listPtr).BootLaunch != BOOT_LAUNCH)
							continue;
		
						int opState;
						bool degraded;
						oam.getModuleStatus(config.moduleName(), opState, degraded);
						if (opState == oam::FAILED) {
							requestStatus = oam::API_FAILURE;
							break;
						}

						//check the process current status & start the requested process
						if ((*listPtr).state == oam::MAN_OFFLINE ||
							(*listPtr).state == oam::AUTO_OFFLINE ||
							(*listPtr).state == oam::COLD_STANDBY ||
							(*listPtr).state == oam::INITIAL) {

							//Check for SIMPLEX runtype processes
							int initType = checkSpecialProcessState( (*listPtr).ProcessName, (*listPtr).RunType, (*listPtr).ProcessModuleType );

							if ( initType == oam::COLD_STANDBY ) {
								//there is a mate active, skip
								(*listPtr).state = oam::COLD_STANDBY;
								requestStatus = API_SUCCESS;
								//sleep(1);
								continue;
							}

							pid_t processID = startProcess((*listPtr).ProcessModuleType,
														(*listPtr).ProcessName,
														(*listPtr).ProcessLocation, 
														(*listPtr).ProcessArgs,
														(*listPtr).launchID,
														(*listPtr).BootLaunch,
														(*listPtr).RunType,
														(*listPtr).DepProcessName, 
														(*listPtr).DepModuleName,
														(*listPtr).LogFile,
														initType);
			
							if ( processID > oam::API_MAX ) {
								processID = oam::API_SUCCESS;
							}

							requestStatus = processID;

							if ( requestStatus != oam::API_SUCCESS ) {
								// error in launching a process
								break;
							}
//							sleep(1);
						}
					}

					if ( requestStatus == oam::API_SUCCESS ) {
						//launched any processes controlled by ProcMgr
						for (listPtr = aPtr->begin(); listPtr != aPtr->end(); ++listPtr)
						{
							if ((*listPtr).BootLaunch != MGR_LAUNCH)
								continue;
			
							int opState;
							bool degraded;
							oam.getModuleStatus(config.moduleName(), opState, degraded);
							if (opState == oam::FAILED) {
								requestStatus = oam::API_FAILURE;
								break;
							}

							//check the process current status & start the requested process
							if ((*listPtr).state == oam::MAN_OFFLINE ||
								(*listPtr).state == oam::AUTO_OFFLINE ||
								(*listPtr).state == oam::COLD_STANDBY ||
								(*listPtr).state == oam::INITIAL) {
			
								//Check for SIMPLEX runtype processes
								int initType = checkSpecialProcessState( (*listPtr).ProcessName, (*listPtr).RunType, (*listPtr).ProcessModuleType );

								if ( initType == oam::COLD_STANDBY ) {
									//there is a mate active, skip
									(*listPtr).state = oam::COLD_STANDBY;
									requestStatus = API_SUCCESS;
									//sleep(1);
									continue;
								}

								pid_t processID = startProcess((*listPtr).ProcessModuleType,
															(*listPtr).ProcessName,
															(*listPtr).ProcessLocation, 
															(*listPtr).ProcessArgs,
															(*listPtr).launchID,
															(*listPtr).BootLaunch,
															(*listPtr).RunType,
															(*listPtr).DepProcessName, 
															(*listPtr).DepModuleName,
															(*listPtr).LogFile,
															initType);
				
								if ( processID > oam::API_MAX )
									processID = oam::API_SUCCESS;

								requestStatus = processID;

								if ( requestStatus != oam::API_SUCCESS )
								{
									// error in launching a process
									if ( requestStatus == oam::API_FAILURE &&
										(*listPtr).RunType == SIMPLEX)
										checkProcessFailover((*listPtr).ProcessName);
									else
										break;
								}
								else
								{
									//run startup test script to perform basic DB sanity testing
									if ( gOAMParentModuleFlag 
											&& (*listPtr).ProcessName == "DBRMWorkerNode" ) {
										if ( runStartupTest() != oam::API_SUCCESS ){
											requestStatus = oam::API_FAILURE_DB_ERROR;
											break;
										}
									}
								}
//								sleep(2);
							}
							else
							{	// if DBRMWorkerNode and ACTIVE, run runStartupTest
								if ( gOAMParentModuleFlag 
										&& (*listPtr).ProcessName == "DBRMWorkerNode" 
										&& (*listPtr).state == oam::ACTIVE) {
									if ( runStartupTest() != oam::API_SUCCESS ){
										requestStatus = oam::API_FAILURE_DB_ERROR;
										break;
									}
								}
							}
						}
					}

					if ( requestStatus == oam::API_SUCCESS ) {

						//check and send noitification
						MonitorConfig config;
						if ( config.moduleType() == "um" )
							oam.sendDeviceNotification(config.moduleName(), UM_ACTIVE);
						else
							if ( gOAMParentModuleFlag )
								oam.sendDeviceNotification(config.moduleName(), PM_MASTER_ACTIVE);
							else
								if (gOAMStandbyModuleFlag)
									oam.sendDeviceNotification(config.moduleName(), PM_STANDBY_ACTIVE);
								else
									oam.sendDeviceNotification(config.moduleName(), PM_COLD_ACTIVE);
					}

					ackMsg << (ByteStream::byte) ACK;
					ackMsg << (ByteStream::byte) STARTALL;
					ackMsg << (ByteStream::byte) requestStatus;
					mq.write(ackMsg);

					log.writeLog(__LINE__, "STARTALL: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));
	
					break;
				}
				case SHUTDOWNMODULE:
				{
					msg >> manualFlag;
					log.writeLog(__LINE__,  "MSG RECEIVED: Shutdown Module request...");

					//Loop reversely thorugh the process list
			
/*					processList* aPtr = config.monitoredListPtr();
					processList::reverse_iterator rPtr;
					uint16_t rtnCode;
		
					for (rPtr = aPtr->rbegin(); rPtr != aPtr->rend(); ++rPtr)
					{
						// don't shut yourself or ProcessManager down"
						if ((*rPtr).ProcessName == "ProcessMonitor" || (*rPtr).ProcessName == "ProcessManager")
							continue;
	
						// update local process state 
						if ( manualFlag )
							(*rPtr).state = oam::MAN_OFFLINE;
						else
							(*rPtr).state = oam::AUTO_OFFLINE;
		
						rtnCode = stopProcess((*rPtr).processID, (*rPtr).ProcessName, (*rPtr).ProcessLocation, actIndicator, manualFlag);
						if (rtnCode)
							log.writeLog(__LINE__,  "Process cannot be stopped:" + (*rPtr).ProcessName, LOG_TYPE_DEBUG);
						else
							(*rPtr).processID = 0;
					}
	
					//send down notification
					oam.sendDeviceNotification(config.moduleName(), MODULE_DOWN);

					//stop the mysql daemon and then infinidb
					try {
						oam.actionMysqlCalpont(MYSQL_STOP);
					}
					catch(...)
					{}
*/
					ackMsg << (ByteStream::byte) ACK;
					ackMsg << (ByteStream::byte) SHUTDOWNMODULE;
					ackMsg << (ByteStream::byte) API_SUCCESS;
					mq.write(ackMsg);

					log.writeLog(__LINE__, "SHUTDOWNMODULE: ACK back to ProcMgr, return status = " + oam.itoa((int) API_SUCCESS));

					//sleep to give time for process-manager to finish up
					sleep(5);
					string cmd = startup::StartUp::installDir() + "/bin/infinidb stop > /dev/null 2>&1";
					system(cmd.c_str());
					exit (0);

					break;
				}


				default:
					break;
			} //end of switch
			break;
		}

		case PROCUPDATELOG:
		{
			string action;
			string level;

			msg >> action;
			msg >> level;

			log.writeLog(__LINE__,  "MSG RECEIVED: " + action + " logging at level " + level);

			uint16_t rtnCode;
			int requestStatus = API_SUCCESS;

			rtnCode = updateLog(action, level);
			if (rtnCode)
				// error in updating log
				requestStatus = API_FAILURE;

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) PROCUPDATELOG;
			ackMsg << (ByteStream::byte) requestStatus;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "PROCUPDATELOG: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

			break;
		}

		case PROCGETCONFIGLOG:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Get Module Log Configuration data");

			int16_t requestStatus = getConfigLog();

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) PROCGETCONFIGLOG;
			ackMsg << (ByteStream::byte) requestStatus;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "PROCGETCONFIGLOG: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

			break;
		}

		case CHECKPOWERON:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Check Power-On Test results log file");
			checkPowerOnResults();

			break;
		}

		case PROCUPDATECONFIG:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Update Process Configuration");

			int16_t requestStatus = updateConfig();

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) PROCUPDATECONFIG;
			ackMsg << (ByteStream::byte) requestStatus;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "PROCUPDATECONFIG: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

			break;
		}

		case PROCBUILDSYSTEMTABLES:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Check and Build System Tables");

			int16_t requestStatus = buildSystemTables();

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) PROCBUILDSYSTEMTABLES;
			ackMsg << (ByteStream::byte) requestStatus;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "PROCBUILDSYSTEMTABLES: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

			break;
		}

		case LOCALHEARTBEAT:
		{
			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) LOCALHEARTBEAT;
			ackMsg << (ByteStream::byte) API_SUCCESS;
			mq.write(ackMsg);
			break;
		}

		case RECONFIGURE:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Reconfigure Module");
			string reconfigureModuleName;
			msg >> reconfigureModuleName;

			uint16_t rtnCode;
			int requestStatus = API_SUCCESS;

			//validate that I should be receiving this message
			if( config.moduleType() != "um" &&
				config.moduleType() != "pm" )
				requestStatus = oam::API_FAILURE;
			else
			{
				if( config.moduleType() == "um" &&
					reconfigureModuleName.find("pm") == string::npos )
					requestStatus = oam::API_FAILURE;
				else
				{
					if( config.moduleType() == "pm" &&
						reconfigureModuleName.find("um") == string::npos )
						requestStatus = oam::API_FAILURE;
					else
					{
						rtnCode = reconfigureModule(reconfigureModuleName);
						if (rtnCode)
							// error in updating log
							requestStatus = rtnCode;
					}
				}
			}

			// remove mysql rpms if being reconfigured as a pm
			if ( reconfigureModuleName.find("pm") != string::npos &&
				config.ServerInstallType() != oam::INSTALL_COMBINE_DM_UM_PM ) {
				try {
					oam.actionMysqlCalpont(MYSQL_STOP);
				}
				catch(...)
				{}
				system("rpm -e infinidb-storage-engine infinidb-mysql");
				system("dpkg -P infinidb-storage-engine infinidb-mysql");
				log.writeLog(__LINE__, "RECONFIGURE: removed mysql rpms");
			}

			// install mysql rpms if being reconfigured as a um
			if ( reconfigureModuleName.find("um") != string::npos ) {
				system("rpm -ivh /root/infinidb-storage-engine-*rpm /root/infinidb-mysql-*rpm > /tmp/rpminstall");
				system("dpgk -i /root/infinidb-storage-engine-*deb /root/infinidb-mysql-*deb >> /tmp/rpminstall");

				string cmd = startup::StartUp::installDir() + "/bin/post-mysqld-install >> /tmp/rpminstall";
				system(cmd.c_str());
				cmd = startup::StartUp::installDir() + "/bin/post-mysql-install >> /tmp/rpminstall";
				system(cmd.c_str());
				system("/etc/init.d/mysql-Calpont start > /tmp/mysqldstart");

				ifstream file ("/tmp/mysqldstart");
				if (!file) {
					requestStatus = oam::API_FAILURE;
					log.writeLog(__LINE__, "RECONFIGURE: mysqld failed to start", LOG_TYPE_ERROR);
				}
				else
				{
					char line[200];
					string buf;
					int count=0;
					while (file.getline(line, 200))
					{
						buf = line;
						if ( buf.find("OK",0) != string::npos )
							count++;
					}
	
					file.close();

					if (count == 0) {
						requestStatus = oam::API_FAILURE;
						log.writeLog(__LINE__, "RECONFIGURE: mysqld failed to start", LOG_TYPE_ERROR);
					}
					else
						log.writeLog(__LINE__, "RECONFIGURE: install mysql rpms and started mysqld");
				}
			}

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) RECONFIGURE;
			ackMsg << (ByteStream::byte) requestStatus;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "RECONFIGURE: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

			//now exit so Process Monitor can restart and reinitialzation as the New module type
			if ( requestStatus != oam::API_FAILURE ) {
				log.writeLog(__LINE__, "RECONFIGURE: ProcMon exiting so it can restart as new module type", LOG_TYPE_DEBUG);
//				sleep(1);

				exit(1);
			}

			break;
		}

		case GETSOFTWAREINFO:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Get Calpont Software Info");

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) GETSOFTWAREINFO;
			ackMsg << config.SoftwareVersion() + config.SoftwareRelease();
			mq.write(ackMsg);

			log.writeLog(__LINE__, "GETSOFTWAREINFO: ACK back to ProcMgr with " + config.SoftwareVersion() + config.SoftwareRelease());

			break;
		}

		case UPDATEPARENTNFS:
		{
			string IPAddress;

			msg >> IPAddress;

			log.writeLog(__LINE__,  "MSG RECEIVED: Update fstab file with new Parent OAM IP of " + IPAddress);

			int requestStatus = API_SUCCESS;

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) UPDATEPARENTNFS;
			ackMsg << (ByteStream::byte) requestStatus;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "UPDATEPARENTNFS: ACK back to ProcMgr");

			break;
		}

		case OAMPARENTACTIVE:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: OAM Parent Activate");

			runStandby = false;

			log.writeLog(__LINE__, "Running Active", LOG_TYPE_INFO);
			//setup TransactionLog on mounted Performance Module
			setTransactionLog(true);

			//give time for Status Control thread to start reading incoming messages
			sleep(3);

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) OAMPARENTACTIVE;
			ackMsg << (ByteStream::byte) API_SUCCESS;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "OAMPARENTACTIVE: ACK back to ProcMgr");

			MonitorConfig config;
			break;
		}

		case UPDATECONFIGFILE:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Update Calpont Config file");

			(void)updateConfigFile(msg);

			log.writeLog(__LINE__, "UPDATECONFIGFILE: Completed");

			MonitorConfig config;
			break;
		}

		case GETPARENTOAMMODULE:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Get Parent OAM Module");

			MonitorConfig config;
			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) GETPARENTOAMMODULE;
			ackMsg << config.OAMParentName();
			mq.write(ackMsg);

			log.writeLog(__LINE__, "GETPARENTOAMMODULE: ACK back with " + config.OAMParentName());

			break;
		}

		case OAMPARENTCOLD:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: OAM Parent Standby ");

			runStandby = true;

			// delete any old active alarm log file
			unlink ("/var/log/Calpont/activeAlarms");

			log.writeLog(__LINE__, "Running Standby", LOG_TYPE_INFO);
			//give time for Status Control thread to start reading incoming messages
			sleep(3);

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) OAMPARENTCOLD;
			ackMsg << (ByteStream::byte) API_SUCCESS;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "OAMPARENTCOLD: ACK back to ProcMgr");

			MonitorConfig config;
			break;
		}

		case UPDATESNMPD:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Update snmpd.conf file ");

			string oldIPAddr;
			string newIPAddr;

			msg >> oldIPAddr;
			msg >> newIPAddr;

			//update snmpd.conf
			SNMPManager sm;
			sm.updateSNMPD(oldIPAddr, newIPAddr);
		
			//reinit SNMPAgent
			system("pkill -HUP snmpd");

			log.writeLog(__LINE__, "UPDATESNMPD: ACK back to ProcMgr");

			break;
		}

		case RUNUPGRADE:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Run upgrade script ");

			string mysqlpw;
			msg >> mysqlpw;

			// run upgrade script
			int ret = runUpgrade(mysqlpw);

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) RUNUPGRADE;
			ackMsg << (ByteStream::byte) ret;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "RUNUPGRADE: ACK back to ProcMgr return status = " + oam.itoa((int) ret));

			break;
		}

		case PROCUNMOUNT:
		{
			string dbrootID;
			msg >> dbrootID;

			log.writeLog(__LINE__,  "MSG RECEIVED: Unmount dbroot: " + dbrootID);

			//Flush the cache
			cacheutils::flushPrimProcCache();
			cacheutils::dropPrimProcFdCache();
			flushInodeCache();

			int return_status = API_SUCCESS;
			if (GlusterConfig == "n")
			{
				int retry = 1; 
				for ( ; retry < 5 ; retry++)
				{
					//if dbroot1, stop syslog
					if ( dbrootID == "1")
						oam.syslogAction("stop");

					string cmd = "umount " + startup::StartUp::installDir() + "/data" + dbrootID + " > /tmp/umount.txt 2>&1";

					system(cmd.c_str());
		
					return_status = API_SUCCESS;
					if (!oam.checkLogStatus("/tmp/umount.txt", "busy"))
						break;

					if ( rootUser) {
						cmd = "lsof " + startup::StartUp::installDir() + "/data" + dbrootID + " >> /tmp/umount.txt 2>&1";
						system(cmd.c_str());
						cmd = "fuser -muvf " + startup::StartUp::installDir() + "/data" + dbrootID + " >> /tmp/umount.txt 2>&1";
						system(cmd.c_str());	
					}
					else
					{
						cmd = "sudo lsof " + startup::StartUp::installDir() + "/data" + dbrootID + " >> /tmp/umount.txt 2>&1";
						system(cmd.c_str());
						cmd = "sudo fuser -muvf " + startup::StartUp::installDir() + "/data" + dbrootID + " >> /tmp/umount.txt 2>&1";
						system(cmd.c_str());
					}

					sleep(2);
					//Flush the cache
					cacheutils::flushPrimProcCache();
					cacheutils::dropPrimProcFdCache();
					flushInodeCache();
				}

				//if dbroot1, stop syslog
				if ( dbrootID == "1")
					oam.syslogAction("start");

				if ( retry >= 5 )
				{
					log.writeLog(__LINE__, "Unmount failed, device busy, dbroot: " + dbrootID, LOG_TYPE_ERROR);
					return_status = API_FAILURE;
					system("mv -f /tmp/umount.txt /tmp/umount_failed.txt");
				}
			}
			else
			{
				try {
					string moduleid = oam.itoa(config.moduleID());
					string errmsg;
					int ret = oam.glusterctl(oam::GLUSTER_UNASSIGN, dbrootID, moduleid, errmsg);
					if ( ret != 0 )
						log.writeLog(__LINE__, "Error unassigning gluster dbroot# " + dbrootID + ", error: " + errmsg, LOG_TYPE_ERROR);
					else
						log.writeLog(__LINE__, "Gluster unassign gluster dbroot# " + dbrootID);
				}
				catch (...)
				{
					log.writeLog(__LINE__, "Exception unassigning gluster dbroot# " + dbrootID, LOG_TYPE_ERROR);
				}
			}

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) PROCUNMOUNT;
			ackMsg << (ByteStream::byte) return_status;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "PROCUNMOUNT: ACK back to ProcMgr, status: " + oam.itoa(return_status));

			break;
		}

		case PROCMOUNT:
		{
			string dbrootID;
			msg >> dbrootID;

			log.writeLog(__LINE__,  "MSG RECEIVED: Mount dbroot: " + dbrootID);;

			int return_status = API_SUCCESS;
			if (GlusterConfig == "n")
			{
				string cmd = "mount " + startup::StartUp::installDir() + "/data" + dbrootID + " > /tmp/mount.txt 2>&1";
				system(cmd.c_str());
	
				if ( !rootUser) {
					cmd = "sudo chown -R " + USER + ":" + USER + " " + startup::StartUp::installDir() + "/data" + dbrootID + " > /dev/null";
					system(cmd.c_str());
				}

				return_status = API_SUCCESS;
				ifstream in("/tmp/mount.txt");
		
				in.seekg(0, std::ios::end);
				int size = in.tellg();
				if ( size != 0 )
				{
					if (!oam.checkLogStatus("/tmp/mount.txt", "already")) {
						log.writeLog(__LINE__, "mount failed, dbroot: " + dbrootID, LOG_TYPE_ERROR);
						return_status = API_FAILURE;
						system("mv -f /tmp/mount.txt /tmp/mount_failed.txt");
					}
				}
			}

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) PROCMOUNT;
			ackMsg << (ByteStream::byte) return_status;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "PROCMOUNT: ACK back to ProcMgr, status: " + oam.itoa(return_status));

			break;
		}

		case PROCFSTABUPDATE:
		{
			string entry;
			msg >> entry;

			string cmd = "echo " + entry + " >> /etc/fstab";
			system(cmd.c_str());

			log.writeLog(__LINE__, "Add line entry to /etc/fstab : " + entry);

			//mkdir on entry directory
			string::size_type pos = entry.find(" ",0);
			string::size_type pos1 = entry.find(" ",pos+1);
			string directory = entry.substr(pos+1,pos1-pos);
			cmd = "mkdir " + directory;
			system(cmd.c_str());
			log.writeLog(__LINE__, "create directory: " + directory);

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) PROCFSTABUPDATE;
			ackMsg << (ByteStream::byte) API_SUCCESS;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "PROCFSTABUPDATE: ACK back to ProcMgr");

			break;
		}

		case MASTERREP:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Run Master Replication script ");

			string mysqlpw;
			msg >> mysqlpw;

			string masterLogFile = oam::UnassignedName;
			string masterLogPos = oam::UnassignedName;

			//change local my.cnf file
			int ret = changeMyCnf("master");

			if ( ret == oam::API_FAILURE )
			{
				ackMsg << (ByteStream::byte) ACK;
				ackMsg << (ByteStream::byte) MASTERREP;
				ackMsg << (ByteStream::byte) ret;
				ackMsg <<  masterLogFile;
				ackMsg <<  masterLogPos;
				mq.write(ackMsg);
	
				log.writeLog(__LINE__, "MASTERREP: Error in changeMyCnf - ACK back to ProcMgr return status = " + oam.itoa((int) ret));
				break;
			}

			// run Master Rep script
			ret = runMasterRep(mysqlpw, masterLogFile, masterLogPos);

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) MASTERREP;
			ackMsg << (ByteStream::byte) ret;
			ackMsg <<  masterLogFile;
			ackMsg <<  masterLogPos;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "MASTERREP: ACK back to ProcMgr return status = " + oam.itoa((int) ret));

			break;
		}

		case SLAVEREP:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Run Slave Replication script ");

			string mysqlpw;
			msg >> mysqlpw;
			string masterLogFile;
			msg >> masterLogFile;
			string masterLogPos;
			msg >> masterLogPos;

			//change local my.cnf file
			int ret = changeMyCnf("slave");

			if ( ret == oam::API_FAILURE )
			{
				ackMsg << (ByteStream::byte) ACK;
				ackMsg << (ByteStream::byte) SLAVEREP;
				ackMsg << (ByteStream::byte) ret;
				mq.write(ackMsg);
	
				log.writeLog(__LINE__, "SLAVEREP: Error in changeMyCnf - ACK back to ProcMgr return status = " + oam.itoa((int) ret));
				break;
			}

			// run Slave Rep script
			ret = runSlaveRep(mysqlpw, masterLogFile, masterLogPos);

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) SLAVEREP;
			ackMsg << (ByteStream::byte) ret;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "SLAVEREP: ACK back to ProcMgr return status = " + oam.itoa((int) ret));

			break;
		}

		case MASTERDIST:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Run Master DB Distribute command ");

			string password;
			msg >> password;
			string module;
			msg >> module;

			// run Master Dist 
			int ret = runMasterDist(password, module);

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) MASTERDIST;
			ackMsg << (ByteStream::byte) ret;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "MASTERDIST: Error in runMasterRep - ACK back to ProcMgr return status = " + oam.itoa((int) ret));

			break;
		}

		default:
			break;
	} //end of switch
	return;
}

/******************************************************************************************
* @brief	stopProcess
*
* purpose:	stop a process
*
******************************************************************************************/
int ProcessMonitor::stopProcess(pid_t processID, std::string processName, std::string processLocation, int actionIndicator, bool manualFlag)
{
	int status;
	MonitorLog log;
	Oam oam;

	log.writeLog(__LINE__, "STOPPING Process: " + processName, LOG_TYPE_DEBUG);

	sendAlarm(processName, PROCESS_INIT_FAILURE, CLEAR);

	if ( manualFlag ) {
		// Mark the process offline 
		updateProcessInfo(processName, oam::MAN_OFFLINE, 0);

		// Set the alarm 
		sendAlarm(processName, PROCESS_DOWN_MANUAL, SET);
	
	}
	else
	{
		// Mark the process offline 
		updateProcessInfo(processName, oam::AUTO_OFFLINE, 0);

		// Set the alarm 
		sendAlarm(processName, PROCESS_DOWN_AUTO, SET);	
	}

	// bypass if pid = 0
	if ( processID == 0 )
		status = API_SUCCESS;
	else
	{
		if (actionIndicator == GRACEFUL)
		{
			status = kill(processID, SIGTERM);
		}
		else
		{
			status = kill(processID, SIGKILL);
		}
	
		// processID not found, set as success	
		if ( errno == ESRCH)
			status = API_SUCCESS;
	
		if ( status != API_SUCCESS)
		{
			status = errno;
			log.writeLog(__LINE__, "Failure to stop Process: " + processName + ", error = " + oam.itoa(errno), LOG_TYPE_ERROR );
		}
	}

	//now do a pkill on process just to make sure all is clean
	string::size_type pos = processLocation.find("bin/",0);
	string procName = processLocation.substr(pos+4, 15) + "\\*";
	string cmd = "pkill -9 " + procName;
	system(cmd.c_str());
	log.writeLog(__LINE__, "Pkill Process just to make sure: " + procName, LOG_TYPE_DEBUG);

	return status;
}

/******************************************************************************************
* @brief	startProcess
*
* purpose:	Start a process
*
******************************************************************************************/
pid_t ProcessMonitor::startProcess(string processModuleType, string processName, string processLocation, 
		string arg_list[MAXARGUMENTS], uint16_t launchID, uint16_t BootLaunch, 
		string RunType, string DepProcessName[MAXDEPENDANCY] , 
		string DepModuleName[MAXDEPENDANCY], string LogFile, uint16_t initType,  uint16_t actIndicator)
{
	pid_t  newProcessID;
	char* argList[MAXARGUMENTS];
	unsigned int i = 0;
	MonitorLog log;
	unsigned int numAugs = 0;
	Oam oam;
	SystemProcessStatus systemprocessstatus;
	ProcessStatus processstatus;

	log.writeLog(__LINE__, "STARTING Process: " + processName, LOG_TYPE_DEBUG);
	log.writeLog(__LINE__, "Process location: " + processLocation, LOG_TYPE_DEBUG);

	//check process location
	if (access(processLocation.c_str(), X_OK) != 0) {
		log.writeLog(__LINE__, "Process location: " + processLocation + " not found", LOG_TYPE_ERROR);

		//record the process information into processList 
		config.buildList(processModuleType, processName, processLocation, arg_list, 
							launchID, newProcessID, FAILED, BootLaunch, RunType,
							DepProcessName, DepModuleName, LogFile);

		//Update Process Status: Mark Process INIT state 
		updateProcessInfo(processName, FAILED, newProcessID);

		return oam::API_FAILURE;
	}

	//check process dependency 
	if (DepProcessName[i].length() != 0)
	{
		for (int i = 0; i < MAXDEPENDANCY; i++)
		{
			//Get dependent process status 
			if (DepProcessName[i].length() == 0)
			{
				break; 
			}

			//check for System wild card on Module Name
			if ( DepModuleName[i] == "*" ) {
				//check for all accurrances of this Module Name
				try
				{
					oam.getProcessStatus(systemprocessstatus);
			
					for( unsigned int j = 0 ; j < systemprocessstatus.processstatus.size(); j++)
					{
						if ( systemprocessstatus.processstatus[j].ProcessName == DepProcessName[i] ) {

							log.writeLog(__LINE__, "Dependent process of " + DepProcessName[i] + "/" + systemprocessstatus.processstatus[j].Module + " is " + oam.itoa(systemprocessstatus.processstatus[j].ProcessOpState), LOG_TYPE_DEBUG);

							int opState;
							bool degraded;
							oam.getModuleStatus(systemprocessstatus.processstatus[j].Module, opState, degraded);
							if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED || 
								opState == oam::AUTO_OFFLINE)
								continue;

							if (systemprocessstatus.processstatus[j].ProcessOpState != oam::ACTIVE )
							{
								log.writeLog(__LINE__, "Dependent Process is not in correct state, Failed Restoral", LOG_TYPE_DEBUG);
								return oam::API_MINOR_FAILURE;
							}
						}
					}
				}
				catch (exception& ex)
				{
					string error = ex.what();
					log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
					return oam::API_MINOR_FAILURE;
				}
				catch(...)
				{
					log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
					return oam::API_MINOR_FAILURE;
				}
			}
			else
			{
				//check for a Module Type wildcard
				if( DepModuleName[i].find("*") != string::npos) {
					string moduleName = DepModuleName[i].substr(0,MAX_MODULE_TYPE_SIZE);
	
					try
					{
						oam.getProcessStatus(systemprocessstatus);
				
						for( unsigned int j = 0 ; j < systemprocessstatus.processstatus.size(); j++)
						{
							if ( systemprocessstatus.processstatus[j].ProcessName == DepProcessName[i]
								&&  systemprocessstatus.processstatus[j].Module.find(moduleName,0) != string::npos) {

								log.writeLog(__LINE__, "Dependent process of " + DepProcessName[i] + "/" + systemprocessstatus.processstatus[j].Module + " is " + oam.itoa(systemprocessstatus.processstatus[j].ProcessOpState), LOG_TYPE_DEBUG);

								int opState;
								bool degraded;
								oam.getModuleStatus(systemprocessstatus.processstatus[j].Module, opState, degraded);
								if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED || 
									opState == oam::AUTO_OFFLINE)
									continue;

								if (systemprocessstatus.processstatus[j].ProcessOpState != oam::ACTIVE )
								{
									log.writeLog(__LINE__, "Dependent Process is not in correct state, Failed Restoral", LOG_TYPE_DEBUG);
									return oam::API_MINOR_FAILURE;
								}
							}
						}
					}
					catch (exception& ex)
					{
						string error = ex.what();
						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
						return oam::API_MINOR_FAILURE;
					}
					catch(...)
					{
						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
						return oam::API_MINOR_FAILURE;
					}
				}
				else
				{
					//check for a Current Module wildcard
					if( DepModuleName[i] == "@") {
						int state = 0;
						try {
							ProcessStatus procstat;
							oam.getProcessStatus(DepProcessName[i], 
												config.moduleName(), 
												procstat);
							state = procstat.ProcessOpState;
						}
						catch (exception& ex)
						{
							string error = ex.what();
							log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
							return oam::API_MINOR_FAILURE;
						}
						catch(...)
						{
							log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
							return oam::API_MINOR_FAILURE;
						}
			
						log.writeLog(__LINE__, "Dependent process of " + DepProcessName[i] + "/" + config.moduleName() + " is " + oam.itoa(state), LOG_TYPE_DEBUG);
			
						if (state == oam::FAILED) {
							log.writeLog(__LINE__, "Dependent Process is FAILED state, Hard Failed Restoral", LOG_TYPE_DEBUG);

							//record the process information into processList 
							config.buildList(processModuleType, processName, processLocation, arg_list, 
												launchID, newProcessID, FAILED, BootLaunch, RunType,
												DepProcessName, DepModuleName, LogFile);
					
							//Update Process Status: Mark Process INIT state 
							updateProcessInfo(processName, FAILED, newProcessID);

							return oam::API_FAILURE;
						}

						if (state != oam::ACTIVE) {
							log.writeLog(__LINE__, "Dependent Process is not in correct state, Failed Restoral", LOG_TYPE_DEBUG);
							return oam::API_MINOR_FAILURE;
						}
					}
					else
					{
						// specific module name and process dependency
						int state = 0;
						try {
							ProcessStatus procstat;
							oam.getProcessStatus(DepProcessName[i], 
												DepModuleName[i], 
												procstat);
							state = procstat.ProcessOpState;
						}
						catch (exception& ex)
						{
							string error = ex.what();
							log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
							return oam::API_MINOR_FAILURE;
						}
						catch(...)
						{
							log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
							return oam::API_MINOR_FAILURE;
						}
			
						log.writeLog(__LINE__, "Dependent process of " + DepProcessName[i] + "/" + DepModuleName[i] + " is " + oam.itoa(state), LOG_TYPE_DEBUG);
			
						int opState;
						bool degraded;
						oam.getModuleStatus(DepModuleName[i], opState, degraded);
						if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED || 
							opState == oam::AUTO_OFFLINE)
							continue;

						if (state != oam::ACTIVE) {
							log.writeLog(__LINE__, "Dependent Process is not in correct state, Failed Restoral", LOG_TYPE_DEBUG);
							return oam::API_MINOR_FAILURE;
						}
					}
				}
			}
		}//end of FOR
	}

	//Don't start certain processes if local module isn't Parent OAM PM
	if ( processName == "SNMPTrapDaemon" || 
			processName == "DBRMControllerNode" ) {
		if (!gOAMParentModuleFlag) {
			log.writeLog(__LINE__, "Fail Restoral, not on Parent OAM module", LOG_TYPE_ERROR);
			//local PM doesn't have the read/write mount
			return oam::API_MINOR_FAILURE;
		}
	}

	for (i=0; i < MAXARGUMENTS - 1; i++)
	{
		if (arg_list[i].length() == 0)
			break;

		//check if workernode argument, if so setup argument #2 as the slave ID for this module
		string::size_type pos = arg_list[i].find("DBRM_Worker",0);
		if (pos != string::npos) {
			try {
				int slavenodeID = oam.getLocalDBRMID(config.moduleName());
				arg_list[i] = "DBRM_Worker" + oam.itoa(slavenodeID);
				log.writeLog(__LINE__, "getLocalDBRMID Worker Node ID = " + oam.itoa(slavenodeID), LOG_TYPE_DEBUG);
			}
			catch(...)
			{
				log.writeLog(__LINE__, "EXCEPTION ERROR on getLocalDBRMID: no DBRM for module", LOG_TYPE_ERROR);

				//record the process information into processList 
				config.buildList(processModuleType, processName, processLocation, arg_list, 
									launchID, newProcessID, FAILED, BootLaunch, RunType,
									DepProcessName, DepModuleName, LogFile);
		
				//Update Process Status: Mark Process INIT state 
				updateProcessInfo(processName, FAILED, newProcessID);

				return oam::API_FAILURE;
			}
		}


		argList[i] = new char[arg_list[i].length()+1];

		strcpy(argList[i], arg_list[i].c_str()) ;
//		log.writeLog(__LINE__, "Arg list ");
//		log.writeLog(__LINE__, argList[i]);
		numAugs++;
	}
	argList[i] = NULL;

	//run load-brm script before brm processes started
	if ( actIndicator != oam::GRACEFUL) {
		if ( ( gOAMParentModuleFlag && processName == "DBRMControllerNode") ||
				( !gOAMParentModuleFlag && processName == "DBRMWorkerNode") ) {
			string DBRMDir;
//			string tempDBRMDir = startup::StartUp::installDir() + "/data/dbrm";
	
			// get DBRMroot config setting
			string DBRMroot;
			oam.getSystemConfig("DBRMRoot", DBRMroot);
	
			string::size_type pos = DBRMroot.find("/BRM_saves",0);
			if (pos != string::npos)
				//get directory path
				DBRMDir = DBRMroot.substr(0,pos);
			else 
			{
				log.writeLog(__LINE__, "Error: /BRM_saves not found in DBRMRoot config setting", LOG_TYPE_CRITICAL);

				//record the process information into processList 
				config.buildList(processModuleType, processName, processLocation, arg_list, 
									launchID, newProcessID, FAILED, BootLaunch, RunType,
									DepProcessName, DepModuleName, LogFile);
		
				//Update Process Status: Mark Process INIT state 
				updateProcessInfo(processName, FAILED, newProcessID);

				return oam::API_FAILURE;
			}
	
			//create dbrm directory, just to make sure its there
			string cmd = "mkdir -p " + DBRMDir + " > /dev/null 2>&1";
			system(cmd.c_str());
	
			// if Non Parent OAM Module, get the dbmr data from Parent OAM Module
			if ( !gOAMParentModuleFlag && !HDFS ) {
	
				//create temp dbrm directory
//				string cmd = "mkdir " + tempDBRMDir + " > /dev/null 2>&1";
//				system(cmd.c_str());
	
				//setup softlink for editem on the 'um' or shared-nothing non active pm
/*				if( config.moduleType() == "um" || 
					(config.moduleType() == "pm") ) {
					cmd = "mv -f " + DBRMDir + " /root/ > /dev/null 2>&1";
					system(cmd.c_str());
		
					cmd = "ln -s " + tempDBRMDir + " " + DBRMDir + " > /dev/null 2>&1";
					system(cmd.c_str());
				}
*/	
				//change DBRMDir to temp DBRMDir
//				DBRMDir = tempDBRMDir;

				// remove all files for temp directory
				cmd = "rm -f " + DBRMDir + "/*";
				system(cmd.c_str());

				// go request files from parent OAM module
				if ( getDBRMdata() != oam::API_SUCCESS ) {
					log.writeLog(__LINE__, "Error: getDBRMdata failed", LOG_TYPE_ERROR);
					sendAlarm("DBRM", DBRM_LOAD_DATA_ERROR, SET);	
					return oam::API_MINOR_FAILURE;
				}

				sendAlarm("DBRM", DBRM_LOAD_DATA_ERROR, CLEAR);	
				// change DBRMroot to temp DBRMDir path
//				DBRMroot = tempDBRMDir + "/BRM_saves";
			}
	
			//
			// run the 'load_brm' script first if files exist
			//
			string loadScript = "load_brm";
	
			string fileName = DBRMroot + "_current";

			ssize_t fileSize = IDBPolicy::size(fileName.c_str());
			boost::scoped_ptr<IDBDataFile> oldFile(IDBDataFile::open(
											IDBPolicy::getType(fileName.c_str(),
											IDBPolicy::WRITEENG),
											fileName.c_str(), "r", 0));
			if (oldFile && fileSize > 0) {
				char line[200] = {0};
				oldFile->pread(line, 0, fileSize - 1);  // skip the \n
				line[fileSize] = '\0';  // not necessary, but be sure.
				string dbrmFile = line;
	
//				if ( !gOAMParentModuleFlag ) {
	
//					string::size_type pos = dbrmFile.find("/BRM_saves",0);
//					if (pos != string::npos)
//						dbrmFile = tempDBRMDir + dbrmFile.substr(pos,80);;
//				}
	
				string logdir("/var/log/Calpont");
				if (access(logdir.c_str(), W_OK) != 0) logdir = "/tmp";
				string cmd = startup::StartUp::installDir() + "/bin/reset_locks > " + logdir + "/reset_locks.log1 2>&1";
				system(cmd.c_str());
				log.writeLog(__LINE__, "BRM reset_locks script run", LOG_TYPE_DEBUG);
	
//				cmd = startup::StartUp::installDir() + "/bin/clearShm -c  > /dev/null 2>&1";
//				system(cmd.c_str());
//				log.writeLog(__LINE__, "Clear Shared Memory script run", LOG_TYPE_DEBUG);
	
				cmd = startup::StartUp::installDir() + "/bin/" + loadScript + " " + dbrmFile + " > " + logdir + "/load_brm.log1 2>&1";
				log.writeLog(__LINE__, loadScript + " cmd = " + cmd, LOG_TYPE_DEBUG);
				system(cmd.c_str());
				
				cmd = logdir + "/load_brm.log1";
				if (oam.checkLogStatus(cmd, "OK"))
					log.writeLog(__LINE__, "Successfully return from " + loadScript, LOG_TYPE_DEBUG);
				else {
					log.writeLog(__LINE__, "Error return DBRM " + loadScript, LOG_TYPE_ERROR);
					sendAlarm("DBRM", DBRM_LOAD_DATA_ERROR, SET);

					//record the process information into processList 
					config.buildList(processModuleType, processName, processLocation, arg_list, 
										launchID, 0, FAILED, BootLaunch, RunType,
										DepProcessName, DepModuleName, LogFile);
			
					//Update Process Status: Mark Process FAILED state 
					updateProcessInfo(processName, FAILED, 0);

					return oam::API_FAILURE;
				}
	
				// now delete the dbrm data from local disk
				if ( !gOAMParentModuleFlag && !HDFS ) {
					string cmd = "rm -f " + DBRMDir + "/*";
					system(cmd.c_str());
					log.writeLog(__LINE__, "removed DBRM file with command: " + cmd, LOG_TYPE_DEBUG);
				}
			}
			else
				log.writeLog(__LINE__, "No DBRM files exist, must be a initial startup", LOG_TYPE_DEBUG);
		}
	
		sendAlarm("DBRM", DBRM_LOAD_DATA_ERROR, CLEAR);
	}

	//do a pkill on process just to make sure there is no rouge version running
	string::size_type pos = processLocation.find("bin/",0);
	string procName = processLocation.substr(pos+4, 15) + "\\*";
	string cmd = "pkill -9 " + procName;
	system(cmd.c_str());
	log.writeLog(__LINE__, "Pkill Process just to make sure: " + procName, LOG_TYPE_DEBUG);

	//Update Process Status: Mark Process INIT state
	updateProcessInfo(processName, initType, 0);

	//sleep, give time for INIT state to be update, prevent race condition with ACTIVE
	sleep(1);

	//delete any old pid file for snmp processes
	if (processLocation.find("snmp") != string::npos)
	{
		string pidFileLocation = argList[numAugs-1];
		unlink (pidFileLocation.c_str());
	}

	//check and setup for logfile
	time_t now;
	now = time(NULL);
	struct tm tm;
	localtime_r(&now, &tm);
	char timestamp[200];
	strftime (timestamp, 200, "%m:%d:%y-%H:%M:%S", &tm);

	string logdir("/var/log/Calpont");
	if (access(logdir.c_str(), W_OK) != 0) logdir = "/tmp";
	string outFileName = logdir + "/" + processName + ".out";
	string errFileName = logdir + "/" + processName + ".err";

	string saveoutFileName = outFileName + "." + timestamp + ".log1";
	string saveerrFileName = errFileName + "." + timestamp + ".log1";

	if ( LogFile == "off" ) {
		string cmd = "mv " + outFileName + " " + saveoutFileName;
		system(cmd.c_str());
		cmd = "mv " + errFileName + " " + saveerrFileName;
		system(cmd.c_str());
	}
	else
	{
		string cmd = "mv " + outFileName + " " + saveoutFileName;
		system(cmd.c_str());
		cmd = "mv " + errFileName + " " + saveerrFileName;
		system(cmd.c_str());
	}

	//fork and exec new process
	newProcessID = fork();

	if (newProcessID != 0)
	{
		//
		// parent processing
		//

		if ( newProcessID == -1)
		{
			log.writeLog(__LINE__, "New Process ID = -1, failed StartProcess", LOG_TYPE_DEBUG);
			return oam::API_MINOR_FAILURE;
		}

		if (processLocation.find("DecomSvr") != string::npos)
		{	// DecomSvr app is special

			sleep(1);
			//record the process information into processList 
			config.buildList(processModuleType, processName, processLocation, arg_list, 
								launchID, newProcessID, oam::ACTIVE, BootLaunch, RunType,
								DepProcessName, DepModuleName, LogFile);
	
			//Update Process Status: Mark Process oam::ACTIVE state 
			updateProcessInfo(processName, oam::ACTIVE, newProcessID);
		}

		if (processLocation.find("snmp") != string::npos)
		{	// snmp app is special...........
			// wait for up to 30 seconds for the pid file to be created

			//get snmp app pid which was stored when app was launched
			//the location is the last augument
			// open sometimes fail, so put in a retry loop

			newProcessID = 0;
			string pidFileLocation = argList[numAugs-1];

			int i;
			for (i=0; i < 30 ; i++)
			{
				sleep(1);
				ifstream f(pidFileLocation.c_str(), ios::in);
					
				if (f.good())
				{	
					// store pid from PID file
					f >> newProcessID;
					//retry if pid is 0
					if (newProcessID == 0)
						continue;

					break;
				}
			}
			if (i == 30) {
				log.writeLog(__LINE__, "FAILURE: no valid PID stored in " + pidFileLocation, LOG_TYPE_ERROR);
				return oam::API_MINOR_FAILURE;
			}

			//record the process information into processList 
			config.buildList(processModuleType, processName, processLocation, arg_list, 
								launchID, newProcessID, oam::ACTIVE, BootLaunch, RunType,
								DepProcessName, DepModuleName, LogFile);
	
			//Update Process Status: Mark Process oam::ACTIVE state 
			updateProcessInfo(processName, oam::ACTIVE, newProcessID);

		}
		else
		{
			//FYI - NEEDS TO STAY HERE TO HAVE newProcessID
	
			//record the process information into processList 
			config.buildList(processModuleType, processName, processLocation, arg_list, 
								launchID, newProcessID, initType, BootLaunch, RunType,
								DepProcessName, DepModuleName, LogFile);
	
			//Update Process Status: Update PID
			updateProcessInfo(processName, STATE_MAX, newProcessID);
		}

		log.writeLog(__LINE__, processName + " PID is " + oam.itoa(newProcessID), LOG_TYPE_DEBUG);

		sendAlarm(processName, PROCESS_DOWN_MANUAL, CLEAR);
		sendAlarm(processName, PROCESS_DOWN_AUTO, CLEAR);
		sendAlarm(processName, PROCESS_INIT_FAILURE, CLEAR);

		//give time to get status updates from process before starting next process
		if ( processName == "DBRMWorkerNode" || processName == "ExeMgr" || processName == "DDLProc")
			sleep(3);
		else
			if ( config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM )
				if ( processName == "PrimProc" || processName == "WriteEngineServer")
					sleep(3);

		for (i=0; i < numAugs; i++)
		{
			if (strlen(argList[i])== 0)
				break;
			delete [] argList[i];
		} 
	}
	else 
	{
		//
		//child processing
		//

		//Close all files opened by parent process
		for (int i = 0; i < sysconf(_SC_OPEN_MAX); i++)
		{
   			close(i);
		}

 		// open STDIN, STDOUT & STDERR for trapDaemon and DecomSvr
		if (processName == "SNMPTrapDaemon" ||
			processName == "DecomSvr" )
		{
			open("/dev/null", O_RDONLY); //Should be fd 0
			open("/dev/null", O_WRONLY); //Should be fd 1
			open("/dev/null", O_WRONLY); //Should be fd 2
		}
		else
		{
			int fd;
			fd = open("/dev/null", O_RDONLY);
			if (fd != 0)
			{
				dup2(fd,0);
				close(fd);
			}

			if ( LogFile == "off" ) {
				fd = open("/dev/null", O_WRONLY); //Should be fd 1
				if ( fd != 1 ) {
					dup2(fd,1);
					close(fd);
				}
				fd = open("/dev/null", O_WRONLY); //Should be fd 2
				if ( fd != 2 ) {
					dup2(fd,2);
					close(fd);
				}
			}
			else
			{
				// open STDOUT & STDERR to log file
//				log.writeLog(__LINE__, "STDOUT  to " + outFileName, LOG_TYPE_DEBUG);
				fd = open(outFileName.c_str(), O_CREAT|O_WRONLY, 0644);
				if (fd != 1)
				{
					dup2(fd,1);
					close(fd);
				}
	
//				log.writeLog(__LINE__, "STDERR  to " + errFileName, LOG_TYPE_DEBUG);
				fd = open(errFileName.c_str(), O_CREAT|O_WRONLY, 0644);
				if (fd != 2)
				{
					dup2(fd,2);
					close(fd);
				}
			}
		}

		//give time to get INIT status updated in shared memory
		sleep(1);
		execv(processLocation.c_str(), argList);

		//record the process information into processList 
		config.buildList(processModuleType, processName, processLocation, arg_list, 
							launchID, newProcessID, FAILED, BootLaunch, RunType,
							DepProcessName, DepModuleName, LogFile);

		//Update Process Status: Mark Process INIT state 
		updateProcessInfo(processName, FAILED, newProcessID);

		exit(oam::API_FAILURE);
	} 

	return newProcessID;
}

/******************************************************************************************
* @brief	reinitProcess
*
* purpose:	re-Init a process, an SNMP agent that will go re-read it's config file
*
******************************************************************************************/
int ProcessMonitor::reinitProcess(pid_t processID, std::string processName, int actionIndicator)
{
	MonitorLog log;

	log.writeLog(__LINE__, "REINITTING Process: " + processName, LOG_TYPE_DEBUG);

	kill(processID, SIGHUP);

	return API_SUCCESS;
}

/******************************************************************************************
* @brief	stopAllProcess
*
* purpose:	Stop all processes started by this monitor
*
******************************************************************************************/
int stopAllProcess(int actionIndicator)
{
	int i;

	if (actionIndicator == GRACEFUL)
	{
		i = kill(0, SIGTERM);

	}
	else
	{
		i = kill(0, SIGKILL);
	}

	if ( i != API_SUCCESS)
	{
		i = errno;
	} 
	
	return i;
}

/******************************************************************************************
* @brief	sendMessage
*
* purpose:	send message to the monitored process or the process manager
*
******************************************************************************************/
int		ProcessMonitor::sendMessage(const string& toWho, const string& message)
{
	int i=0;
	return i;
}

/******************************************************************************************
* @brief	checkHeartBeat
*
* purpose:	check child process heart beat
*
******************************************************************************************/
int		ProcessMonitor::checkHeartBeat(const string processName)
{
	int i=0;
	return i;
}

/******************************************************************************************
* @brief	sendAlarm
*
* purpose:	send a trap and log the process information
*
******************************************************************************************/
void	ProcessMonitor::sendAlarm(string alarmItem, ALARMS alarmID, int action)
{
	MonitorLog log;
	Oam oam;

//        cout << "sendAlarm" << endl;
//       cout << alarmItem << endl;
//        cout << oam.itoa(alarmID) << endl;
//        cout << oam.itoa(action) << endl;


	sendAlarmInfo_t *t1 = new sendAlarmInfo_t;
	*t1 = boost::make_tuple(alarmItem, alarmID, action);

	pthread_t SendAlarmThread;
	int status = pthread_create (&SendAlarmThread, NULL, (void*(*)(void*)) &sendAlarmThread, t1);

	if ( status != 0 )
		log.writeLog(__LINE__, "SendAlarmThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

	return;
}



/******************************************************************************************
* @brief	sendAlarmThread
*
* purpose:	send a trap and log the process information
*
******************************************************************************************/
void 	sendAlarmThread(sendAlarmInfo_t* t)
{
	MonitorLog log;
	Oam oam;
	SNMPManager alarmMgr;

	pthread_mutex_lock(&ALARM_LOCK);

	string alarmItem = boost::get<0>(*t);
	ALARMS alarmID = boost::get<1>(*t);
	int action = boost::get<2>(*t);

	//valid alarmID
	if ( alarmID < 1 || alarmID > oam::MAX_ALARM_ID ) {
		log.writeLog(__LINE__,  "sendAlarmThread error: Invalid alarm ID", LOG_TYPE_DEBUG);

		delete t;
		pthread_mutex_unlock(&ALARM_LOCK);
	
		pthread_exit(0);
	}

	if ( action == SET ) {
		log.writeLog(__LINE__,  "Send SET Alarm ID " + oam.itoa(alarmID) + " on device " + alarmItem, LOG_TYPE_DEBUG);
	}
	else
	{
		log.writeLog(__LINE__,  "Send CLEAR Alarm ID " + oam.itoa(alarmID) + " on device " + alarmItem, LOG_TYPE_DEBUG);
	}

//        cout << "sendAlarmThread" << endl;
//        cout << alarmItem << endl;
//        cout << oam.itoa(alarmID) << endl;
//        cout << oam.itoa(action) << endl;

	try {
		alarmMgr.sendAlarmReport(alarmItem.c_str(), alarmID, action);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on sendAlarmReport", LOG_TYPE_ERROR );
	}

	delete t;
	pthread_mutex_unlock(&ALARM_LOCK);

	pthread_exit(0);
}

/******************************************************************************************
* @brief	updateProcessInfo
*
* purpose:	Send msg to update process state and status change time on disk
*
******************************************************************************************/
bool ProcessMonitor::updateProcessInfo(std::string processName, int state, pid_t PID)
{
	MonitorLog log;
	Oam oam;

	log.writeLog(__LINE__, "StatusUpdate of Process " + processName + " State = " + oam.itoa(state) + " PID = " + oam.itoa(PID), LOG_TYPE_DEBUG);

	sendProcessInfo_t *t1 = new sendProcessInfo_t;
	*t1 = boost::make_tuple(processName, state, PID);

	// if state is offline, use thread for faster results
	if ( state == oam::MAN_OFFLINE || state == oam::AUTO_OFFLINE )
	{
		pthread_t SendProcessThread;
		int status = pthread_create (&SendProcessThread, NULL, (void*(*)(void*)) &sendProcessThread, t1);

		if ( status != 0 )
			log.writeLog(__LINE__, "SendProcessThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);
	}
	else
	{
		sendProcessThread(t1);
	}
	
	return true;
}

/******************************************************************************************
* @brief	sendProcessThread
*
* purpose:	Send msg to update process state and status change time on disk
*
******************************************************************************************/
void sendProcessThread(sendProcessInfo_t* t)
{
	MonitorLog log;
	MonitorConfig config;
	Oam oam;

//	pthread_mutex_lock(&PROCESS_LOCK);

	string processName = boost::get<0>(*t);
	int state = boost::get<1>(*t);
	pid_t PID = boost::get<2>(*t);

	try {
		oam.setProcessStatus(processName, config.moduleName(), state, PID);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on setProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR );
	}

	delete t;
//	pthread_mutex_unlock(&PROCESS_LOCK);

	return;
}

/******************************************************************************************
* @brief	updateLog
*
* purpose:	Enable/Disable Logging configuration within the syslog.conf file
*
*			action: LOG_ENABLE / LOG_DISABLE
*			level:  all, critical, error, warning, info, debug, data
******************************************************************************************/
int ProcessMonitor::updateLog(std::string action, std::string level)
{
	MonitorLog log;
	Oam oam;

	string fileName;
	oam.getSystemConfig("SystemLogConfigFile", fileName);
	if (fileName == oam::UnassignedName ) {
		// unassigned
		log.writeLog(__LINE__, "ERROR: syslog file not configured ", LOG_TYPE_ERROR );
		return -1;
	}

	string::size_type pos = fileName.find("syslog-ng",0);
	if (pos != string::npos) {
		// not supported
		log.writeLog(__LINE__, "ERROR: config file not support, " + fileName, LOG_TYPE_ERROR );
		return -1;
	}

	vector <string> lines;

	if ( level == "data" )
		return 0;

	ifstream oldFile (fileName.c_str());
	if (!oldFile ) {
		// no file found
		log.writeLog(__LINE__, "ERROR: syslog file not found at " + fileName, LOG_TYPE_ERROR );
		return -1;
	}

	//if non-root, change file permissions so we can update it
	if ( !rootUser) {
		string cmd = "sudo chmod 666 " + fileName + " > /dev/null";
		system(cmd.c_str());
	}

	char line[200];
	string buf;
	bool restart=true;

	if( action == oam::ENABLEDSTATE ) {
		// check if enabling all log levels
		if( level == "all") {
			vector <int> fileIDs;
			while (oldFile.getline(line, 200))
			{
				buf = line;

				for( int i = 0;;i++)
				{
					string::size_type pos = oam::LogFile[i].find("local2",0);
					if (pos != string::npos)
						//skip
						continue;

					if ( oam::LogFile[i] == "" ) {
						// end of list
						break;
					}
					string logFile = oam::LogFile[i];

					pos = buf.find(logFile,0);
					if (pos != string::npos) {
						// log file already there, save fileID
						fileIDs.push_back(i);
						break;
					}
				}
				// output to temp file
				lines.push_back(buf);
			} //end of while

			// check which fileIDs aren't in the syslog.conf
			bool update = false;
			for( int i = 0;;i++)
			{
				bool found = false;
				if ( oam::LogFile[i] == "" ) {
					// end of list
					break;
				}
				vector<int>::iterator p = fileIDs.begin();
				while ( p != fileIDs.end() )
				{
					if ( i == *p ) {
						//already there
						found = true;
						break;
					}
					p++;
				}

				if (!found) {
					lines.push_back(oam::LogFile[i]);
					log.writeLog(__LINE__, "Add in syslog.conf log file " + oam::LogFile[i], LOG_TYPE_DEBUG);
					update = true;
				}
			}

			if (!update) {
				log.writeLog(__LINE__, "Log level file's already in syslog.conf", LOG_TYPE_DEBUG);
				restart=false;
			}
		}
		else
		{ // enable a specific level
			// get log file for level
			for( int i = 0;;i++)
			{
				if ( oam::LogLevel[i] == "" ) {
					// end of list
					log.writeLog(__LINE__, "ERROR: log level file not found for level " + level, LOG_TYPE_ERROR );
					oldFile.close();
					return -1;
				}
				if ( level == oam::LogLevel[i] ) {
					// match found
					string logFile = oam::LogFile[i];

					while (oldFile.getline(line, 200))
					{
						buf = line;
						string::size_type pos = buf.find(logFile,0);
						if (pos != string::npos) {
							log.writeLog(__LINE__, "Log level file already in syslog.conf", LOG_TYPE_DEBUG);
							restart=false;
						}
						// output to temp file
						lines.push_back(buf);
					}

					// file not found, add at the bottom of syslog.conf
					lines.push_back(logFile);
					break;
				}
			}
		}
	}
	else
	{ // DISABLE LOG
		// check if disabling all log levels
		if( level == "all") {
			bool update = false;
			while (oldFile.getline(line, 200))
			{
				buf = line;
				bool found = false;
				for( int i = 0;;i++)
				{
					string::size_type pos = oam::LogFile[i].find("local2",0);
					if (pos != string::npos)
						//skip
						continue;

					if ( oam::LogFile[i] == "" ) {
						// end of list
						break;
					}
					string logFile = oam::LogFile[i];

					pos = buf.find(logFile,0);
					if (pos != string::npos) {
						// log file found
						found = true;
						update = true;
						break;
					}
				}
				if (!found)
					// output to temp file
					lines.push_back(buf);
			} //end of while

			if (!update) {
				log.writeLog(__LINE__, "No Log level file's in syslog.conf", LOG_TYPE_DEBUG);
				restart=false;
			}
		}
		else
		{ // disable a specific level
			// get log file for level
			for( int i = 0;;i++)
			{
				if ( oam::LogLevel[i] == "" ) {
					// end of list
					log.writeLog(__LINE__, "ERROR: log level file not found for level " + level, LOG_TYPE_ERROR );
					oldFile.close();
					return -1;
				}
				if ( level == oam::LogLevel[i] ) {
					// match found
					string logFile = oam::LogFile[i];
					bool found = false;
					while (oldFile.getline(line, 200))
					{
						buf = line;
						string::size_type pos = buf.find(logFile,0);
						if (pos != string::npos) {
							// file found, don't push into new file
							log.writeLog(__LINE__, "Log level file to DISABLE found in syslog.conf", LOG_TYPE_DEBUG);
							found = true;
						}
						else
						{
							// no match, push into new temp file
							lines.push_back(buf);
						}
					}
					if (found)
						break;
					else
					{
						log.writeLog(__LINE__, "Log level file not in syslog.conf", LOG_TYPE_DEBUG);
						restart=false;
					}
				}
			}
		}
	}

	oldFile.close();

	//
	//  go write out new file if required
	//

	if (restart) {

		unlink (fileName.c_str());
		ofstream newFile (fileName.c_str());	
		
		// create new file
		int fd = open(fileName.c_str(),O_RDWR|O_CREAT, 0644);
		
		// Aquire an exclusive lock
		if (flock(fd,LOCK_EX) == -1) {
			log.writeLog(__LINE__, "ERROR: file lock failure on " + fileName, LOG_TYPE_ERROR );
			close(fd);
			return -1;
		}
	
		copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
		newFile.close();
		
		// Release lock
		if (flock(fd,LOCK_UN) == -1)
		{
			log.writeLog(__LINE__, "ERROR: file unlock failure on " + fileName, LOG_TYPE_ERROR );
			close(fd);
			return -1;
		}
		close(fd);

		oam.syslogAction("sighup");
	}

	//update file priviledges
	changeModLog();

	return 0;
}

/******************************************************************************************
* @brief	changeModLog
*
* purpose:	Updates the file mods so files can be read/write
* 			from external modules
*
******************************************************************************************/
void ProcessMonitor::changeModLog()
{
	for( int i = 0;;i++)
	{
		if ( oam::LogFile[i].empty() )
			//end of list
			break;

		string logFile = oam::LogFile[i];
		string::size_type pos = logFile.find('/',0);
		logFile = logFile.substr(pos,200);
		string cmd = "chmod 777 " + logFile + " > /dev/null 2>&1";

		system(cmd.c_str());
	}

	return;
}

/******************************************************************************************
* @brief	getConfigLog
*
* purpose:	Get Logging configuration within the syslog.conf file
*
******************************************************************************************/
int ProcessMonitor::getConfigLog()
{
	MonitorLog log;
	Oam oam;

	string fileName;
	oam.getSystemConfig("SystemLogConfigFile", fileName);
	if (fileName == oam::UnassignedName ) {
		// unassigned
		log.writeLog(__LINE__, "ERROR: syslog file not configured ", LOG_TYPE_ERROR );
		return API_FAILURE;
	}

	string::size_type pos = fileName.find("syslog-ng",0);
	if (pos != string::npos) {
		// not supported
		log.writeLog(__LINE__, "ERROR: config file not support, " + fileName, LOG_TYPE_ERROR );
		return API_FAILURE;
	}

	ifstream oldFile (fileName.c_str());
	if (!oldFile) {
		// no file found
		log.writeLog(__LINE__, "ERROR: syslog file not found at " + fileName, LOG_TYPE_ERROR );
		return API_FILE_OPEN_ERROR;
	}

	int configData = 0;
	char line[200];
	string buf;

	while (oldFile.getline(line, 200))
	{
		buf = line;

		for( int i = 0;;i++)
		{
			if ( oam::LogFile[i] == "" ) {
				// end of list
				break;
			}
			string logFile = oam::LogFile[i];

			string::size_type pos = buf.find(logFile,0);
			if (pos != string::npos) {
				// match found
				switch(i+1) {
					case 1:
						configData = configData | LEVEL_CRITICAL;
						break;
					case 2:
						configData = configData | LEVEL_ERROR;
						break;
					case 3:
						configData = configData | LEVEL_WARNING;
						break;
					case 4:
						configData = configData | LEVEL_INFO;
						break;
					case 5:
						configData = configData | LEVEL_DEBUG;
						break;
					case 6:
						configData = configData | LEVEL_DATA;
						break;
					default:
						break;
				} //end of switch
			}
		}
	} //end of while

	oldFile.close();
	
	// adjust data to be different from API RETURN CODES
	configData = configData + API_MAX;

	return configData;
}

/******************************************************************************************
* @brief	checkPowerOnResults
*
* purpose:	Read Power-On TEst results log file and report issues via Alarms
*
******************************************************************************************/
void ProcessMonitor::checkPowerOnResults()
{
	string  POWERON_TEST_RESULTS_FILE = startup::StartUp::installDir() + "/post/st_status";
	MonitorLog log;

	ifstream oldFile (POWERON_TEST_RESULTS_FILE.c_str());
	if (!oldFile) {
		// no file found
		log.writeLog(__LINE__, "ERROR: Power-On Test results file not found at " + POWERON_TEST_RESULTS_FILE, LOG_TYPE_ERROR );
		return;
	}

	int configData = 0;
	char line[200];
	string buf;

	while (oldFile.getline(line, 200))
	{
		buf = line;
		string name;
		string level;
		string info = "";

		// extract name key word
		string::size_type pos = buf.find("name:",0);
		string::size_type pos1;
		if (pos != string::npos) {
			// match found
			pos1 = buf.find("|",pos);
			if (pos1 != string::npos)
				// end of name found
				name = buf.substr(pos+5, pos1-(pos+5));
			else
			{
				// name not found, skip this line
				continue;
			}
		}
		else
		{
			// name not found, skip this line
			continue;
		}

		// extract level key word
		pos = buf.find("level:",pos1);
		if (pos != string::npos) {
			// match found
			pos1 = buf.find("|",pos);
			if (pos1 != string::npos)
				// end of level found
				level = buf.substr(pos+6, pos1-(pos+6));
			else
			{
				// level not found, skip this line
				continue;
			}
		}
		else
		{
			// level not found, skip this line
			continue;
		}

		// extract info key word, if any exist
		pos = buf.find("info:", pos1);
		if (pos != string::npos)
			// match found
			info = buf.substr(pos+5,200);

		// log findings
		LoggingID lid(18);
		MessageLog ml(lid);
		Message msg;
		Message::Args args;
		args.add("Power-On self test: name = ");
		args.add(name);
		args.add(", level = ");
		args.add(level);
		args.add(", info = ");
		args.add(info);
		msg.format(args);
		ml.logDebugMessage(msg);

		// Issue alarm based on level

		pos = level.find("3", 0);
		if (pos != string::npos) {
			// Severe Warning found, Issue alarm 
			sendAlarm(name, POWERON_TEST_SEVERE, SET);

			//Log this event 
			LoggingID lid(18);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("Power-On self test Severe Warning on ");
			args.add(name);
			args.add(": ");
			args.add(info);
			msg.format(args);
			ml.logWarningMessage(msg);
			continue;
		}

		pos = level.find("2", 0);
		if (pos != string::npos) {
			// Warning found, Issue alarm 
			sendAlarm(name, POWERON_TEST_WARNING, SET);

			//Log this event 
			LoggingID lid(18);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("Power-On self test Warning on ");
			args.add(name);
			args.add(": ");
			args.add(info);
			msg.format(args);
			ml.logWarningMessage(msg);
			continue;
		}

		pos = level.find("1", 0);
		if (pos != string::npos) {
			// Info found, Log this event

			LoggingID lid(18);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("Power-On self test Info on ");
			args.add(name);
			args.add(": ");
			args.add(info);
			msg.format(args);
			ml.logInfoMessage(msg);
			continue;
		}

	} //end of while

	oldFile.close();
	
	// adjust data to be different from API RETURN CODES
	configData = configData + API_MAX;

	return;
}

/******************************************************************************************
* @brief	updateConfig
*
* purpose:	Update Config data from disk
*
******************************************************************************************/
int ProcessMonitor::updateConfig()
{
	//ProcMon log file 
	MonitorLog log;
//	MonitorConfig config;
//	ProcessMonitor aMonitor(config, log);
	Oam oam;

	//Read ProcessConfig file to get process list belong to this process monitor 
	SystemProcessConfig systemprocessconfig;
	try {
		oam.getProcessConfig(systemprocessconfig);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: " + error, LOG_TYPE_ERROR );
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: Caught unknown exception!", LOG_TYPE_ERROR );
	}

	//Update a map for application launch ID for this Process-Monitor
	string OAMParentModuleType = config.OAMParentName().substr(0,MAX_MODULE_TYPE_SIZE);
	string systemModuleType = config.moduleName().substr(0,MAX_MODULE_TYPE_SIZE);

	for( unsigned int i = 0 ; i < systemprocessconfig.processconfig.size(); i++)
	{
		if (systemprocessconfig.processconfig[i].ModuleType == systemModuleType
			|| ( systemprocessconfig.processconfig[i].ModuleType == "um" &&
				systemModuleType == "pm" && PMwithUM == "y" )
			|| systemprocessconfig.processconfig[i].ModuleType == "ChildExtOAMModule"
			|| ( systemprocessconfig.processconfig[i].ModuleType == "ChildOAMModule" )
			|| (systemprocessconfig.processconfig[i].ModuleType == "ParentOAMModule" &&
				systemModuleType == OAMParentModuleType) )
		{
			if ( systemprocessconfig.processconfig[i].ModuleType == "um" && 
				systemModuleType == "pm" && PMwithUM == "y" &&
				systemprocessconfig.processconfig[i].ProcessName == "DMLProc" )
				continue;


			if ( systemprocessconfig.processconfig[i].ModuleType == "um" && 
				systemModuleType == "pm" && PMwithUM == "y" &&
				systemprocessconfig.processconfig[i].ProcessName == "DDLProc" )
				continue;

			ProcessStatus processstatus;
			try {
				//Get the process information 
				oam.getProcessStatus(systemprocessconfig.processconfig[i].ProcessName, config.moduleName(), processstatus);
			}
			catch (exception& ex)
			{
				string error = ex.what();
				log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR );
				return API_FAILURE;
			}
			catch(...)
			{
				log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR );
				return API_FAILURE;
			}

			config.buildList(systemprocessconfig.processconfig[i].ModuleType,
					systemprocessconfig.processconfig[i].ProcessName,
					systemprocessconfig.processconfig[i].ProcessLocation,
					systemprocessconfig.processconfig[i].ProcessArgs,
					systemprocessconfig.processconfig[i].LaunchID,
					processstatus.ProcessID,
					processstatus.ProcessOpState,
					systemprocessconfig.processconfig[i].BootLaunch,
					systemprocessconfig.processconfig[i].RunType,
					systemprocessconfig.processconfig[i].DepProcessName, 
					systemprocessconfig.processconfig[i].DepModuleName,
					systemprocessconfig.processconfig[i].LogFile);
		}
	}
	return API_SUCCESS;
}

/******************************************************************************************
* @brief	buildSystemTables
*
* purpose:	Check for and build System Tables if not there
*			Only will be run from 'pm1'
*
******************************************************************************************/
int ProcessMonitor::buildSystemTables()
{
	Oam oam;
	string DBdir;
	oam.getSystemConfig("DBRoot1", DBdir);

	string fileName = DBdir + "/000.dir";

	if (!IDBPolicy::exists(fileName.c_str())) {
		string logdir("/var/log/Calpont");
		if (access(logdir.c_str(), W_OK) != 0) logdir = "/tmp";
		string cmd = startup::StartUp::installDir() + "/bin/dbbuilder 7 > " + logdir + "/dbbuilder.log &";
		system(cmd.c_str());

		log.writeLog(__LINE__, "buildSystemTables: dbbuilder 7 Successfully Launched" , LOG_TYPE_DEBUG);
		return API_SUCCESS;
	}
	log.writeLog(__LINE__, "buildSystemTables: System Tables Already Exist", LOG_TYPE_ERROR );
	return API_FILE_ALREADY_EXIST;
}

/******************************************************************************************
* @brief	reconfigureModule
*
* purpose:	reconfigure Module functionality
*			Edit the moduleFile file with new Module Name
*
******************************************************************************************/
int ProcessMonitor::reconfigureModule(std::string reconfigureModuleName)
{
	Oam oam;
	string installDir = startup::StartUp::installDir();

	//create custom files 
	string dir = installDir + "/local/etc/" + reconfigureModuleName;

	string cmd = "mkdir " + dir + " > /dev/null 2>&1";
	system(cmd.c_str());

	cmd = "rm -f " + dir + "/* > /dev/null 2>&1";
	system(cmd.c_str());

	if ( reconfigureModuleName.find("um") != string::npos) {

		cmd = "cp " + installDir + "/local/etc/um1/* " + dir + "/.";
		system(cmd.c_str());
	}
	else
	{
		cmd = "cp " + installDir + "/local/etc/pm1/* " + dir + "/.";
		system(cmd.c_str());
	}

	//copy and apply new rc.local.calpont from dm1
	cmd = "rm -f " + installDir + "/local/rc.local.calpont";
	system(cmd.c_str());
	cmd = "cp " + installDir + "/local/etc/" + reconfigureModuleName + "/rc.local.calpont " + installDir + "/local/.";
	system(cmd.c_str());
	cmd = "rm -f /etc/rc.d/rc.local";
	system(cmd.c_str());
	cmd = "cp /etc/rc.d/rc.local.calpontSave /etc/rc.d/rc.local >/dev/null 2>&1";
	system(cmd.c_str());
	if (geteuid() == 0)
		cmd = "cat " + installDir + "/local/rc.local.calpont >> /etc/rc.d/rc.local >/dev/null 2>&1";
	system(cmd.c_str());
	cmd = "/etc/rc.d/rc.local >/dev/null 2>&1";
	system(cmd.c_str());

	//update module file
	string fileName = installDir + "/local/module";

	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());

	cmd = "echo " + reconfigureModuleName + " > " + fileName;
	system(cmd.c_str());

	newFile.close();

	return API_SUCCESS;
}


/******************************************************************************************
* @brief	checkSpecialProcessState
*
* purpose:	Check if a SIMPLEX runtype Process has mates already up
*
******************************************************************************************/
int ProcessMonitor::checkSpecialProcessState( std::string processName, std::string runType, string processModuleType )
{
	MonitorLog log;
	MonitorConfig config;
	Oam oam;
	SystemProcessStatus systemprocessstatus;
	ProcessStatus processstatus;
	int retStatus = oam::MAN_INIT;

	if ( runType == SIMPLEX || runType == ACTIVE_STANDBY) {

		log.writeLog(__LINE__, "checkSpecialProcessState on : " + processName, LOG_TYPE_DEBUG);

		if ( runType == SIMPLEX && PMwithUM == "y" && processModuleType == "um" && gOAMParentModuleFlag)
			retStatus = oam::COLD_STANDBY;
		else if ( runType == SIMPLEX && gOAMParentModuleFlag )
			retStatus = oam::MAN_INIT;
		else if ( runType == ACTIVE_STANDBY && processModuleType == "ParentOAMModule" && 
				( gOAMParentModuleFlag || !runStandby ) )
			retStatus = oam::MAN_INIT;
		else if ( runType == ACTIVE_STANDBY && processModuleType == "ParentOAMModule" && config.OAMStandbyParentFlag() )
			retStatus = oam::STANDBY;
		else if ( runType == ACTIVE_STANDBY && processModuleType == "ParentOAMModule" )
			retStatus = oam::COLD_STANDBY;
		else if ( runType == SIMPLEX && processModuleType == "ParentOAMModule" && !gOAMParentModuleFlag)
			retStatus = oam::COLD_STANDBY;
		else
		{
			//simplex on a non um1 or non-parent-pm
			if ( processName == "DMLProc" || processName == "DDLProc" ) {
				string PrimaryUMModuleName;
				try {
					oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);
				}
				catch(...) {}

				if ( PrimaryUMModuleName != config.moduleName() ) {
					retStatus = oam::COLD_STANDBY;
				}
			}

			if ( retStatus != oam::COLD_STANDBY )
			{
				try
				{
					oam.getProcessStatus(systemprocessstatus);
			
					for( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
					{
						if ( systemprocessstatus.processstatus[i].ProcessName == processName  &&
								systemprocessstatus.processstatus[i].Module != config.moduleName() ) {
							if ( systemprocessstatus.processstatus[i].ProcessOpState == ACTIVE || 
									systemprocessstatus.processstatus[i].ProcessOpState == MAN_INIT ||
									systemprocessstatus.processstatus[i].ProcessOpState == AUTO_INIT ||
							//		systemprocessstatus.processstatus[i].ProcessOpState == MAN_OFFLINE ||
							//		systemprocessstatus.processstatus[i].ProcessOpState == INITIAL ||
									systemprocessstatus.processstatus[i].ProcessOpState == BUSY_INIT) {
	
								// found a ACTIVE or going ACTIVE mate
								if ( runType == oam::SIMPLEX )
									// SIMPLEX
									retStatus = oam::COLD_STANDBY;
								else
								{ // ACTIVE_STANDBY
									for( unsigned int j = 0 ; j < systemprocessstatus.processstatus.size(); j++)
									{
										if ( systemprocessstatus.processstatus[j].ProcessName == processName  &&
												systemprocessstatus.processstatus[j].Module != config.moduleName() ) {
											if ( systemprocessstatus.processstatus[j].ProcessOpState == STANDBY ||
													systemprocessstatus.processstatus[j].ProcessOpState == STANDBY_INIT)
												// FOUND ACTIVE AND STANDBY
												retStatus = oam::COLD_STANDBY;
										}
									}
									// FOUND AN ACTIVE, BUT NO STANDBY
									log.writeLog(__LINE__, "checkSpecialProcessState, set STANDBY on " + processName, LOG_TYPE_DEBUG);
									retStatus = oam::STANDBY;
								}
							}
						}
					}
				}
				catch (exception& ex)
				{
					string error = ex.what();
					log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
				}
				catch(...)
				{
					log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
				}
			}
		}
	}

	if ( retStatus == oam::COLD_STANDBY || retStatus == oam::STANDBY ) {
		updateProcessInfo(processName, retStatus, 0);
	
		sendAlarm(processName, PROCESS_DOWN_MANUAL, CLEAR);
		sendAlarm(processName, PROCESS_DOWN_AUTO, CLEAR);
		sendAlarm(processName, PROCESS_INIT_FAILURE, CLEAR);
	}

	log.writeLog(__LINE__, "checkSpecialProcessState status return : " + oam.itoa(retStatus), LOG_TYPE_DEBUG);

	return retStatus;
}

/******************************************************************************************
* @brief	checkMateModuleState
*
* purpose:	Check if Mate Module is Active
*
******************************************************************************************/
int ProcessMonitor::checkMateModuleState()
{
	MonitorLog log;
//	MonitorConfig config;
//	ProcessMonitor aMonitor(config, log);
	Oam oam;
	SystemStatus systemstatus;

	try
	{
		oam.getSystemStatus(systemstatus);

		for( unsigned int i = 0 ; i < systemstatus.systemmodulestatus.modulestatus.size(); i++)
		{
			string moduleName = systemstatus.systemmodulestatus.modulestatus[i].Module;
			string moduleType = moduleName.substr(0,MAX_MODULE_TYPE_SIZE);
			int moduleID = atoi(moduleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());

			if ( moduleType == config.moduleType() && moduleID != config.moduleID() )
				if ( systemstatus.systemmodulestatus.modulestatus[i].ModuleOpState == oam::ACTIVE )
					return API_SUCCESS;
		}
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
		return API_FAILURE;
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
		return API_FAILURE;
	}

	return API_FAILURE;
}

/******************************************************************************************
* @brief	createDataDirs
*
* purpose:	Create the Calpont Data directories
*
*
******************************************************************************************/
int ProcessMonitor::createDataDirs(std::string cloud)
{
	MonitorLog log;
	Oam oam;

	log.writeLog(__LINE__, "createDataDirs called", LOG_TYPE_DEBUG);

	if ( config.moduleType() == "um" && 
		( cloud == "amazon-ec2" || cloud == "amazon-vpc") )
	{
		string UMStorageType;
		try {
			oam.getSystemConfig( "UMStorageType", UMStorageType);
		}
		catch(...) {}
	
		if (UMStorageType == "external")
		{
			if(!amazonVolumeCheck()) {
				//Set the alarm
				sendAlarm(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, SET);
				return API_FAILURE;
			}
		}
	}

	if ( config.moduleType() == "pm" )
	{
		DBRootConfigList dbrootConfigList;
	
		string DBRootStorageType;
		try {
			oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
		}
		catch(...) {}
	
		try
		{
			systemStorageInfo_t t;
			t = oam.getStorageConfig();
	
			if ( boost::get<1>(t) == 0 ) {
				log.writeLog(__LINE__, "No dbroots are configured in Calpont.xml file at proc mon startup time", LOG_TYPE_WARNING);
				return API_INVALID_PARAMETER;
			}
	
			DeviceDBRootList moduledbrootlist = boost::get<2>(t);
	
			DeviceDBRootList::iterator pt = moduledbrootlist.begin();
			for( ; pt != moduledbrootlist.end() ; pt++)
			{
				int moduleID = (*pt).DeviceID;
	
				DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();
				for( ; pt1 != (*pt).dbrootConfigList.end() ;pt1++)
				{
					int id = *pt1;
	
		
					string DBRootName = startup::StartUp::installDir() + "/data" + oam.itoa(id);
		
					string cmd = "mkdir " + DBRootName + " > /dev/null 2>&1";
					int rtnCode = system(cmd.c_str());
					if (WEXITSTATUS(rtnCode) == 0)
						log.writeLog(__LINE__, "Successful created directory " + DBRootName, LOG_TYPE_DEBUG);
		
					cmd = "chmod 1777 " + DBRootName + " > /dev/null 2>&1";
					system(cmd.c_str());

					if ( id == 1 )
					{
						cmd = "mkdir -p " + startup::StartUp::installDir() + "/data1/systemFiles/dbrm > /dev/null 2>&1";
						system(cmd.c_str());

						cmd = "mkdir -p " + startup::StartUp::installDir() + "/data1/systemFiles/dataTransaction > /dev/null 2>&1";
						system(cmd.c_str());
					}

					if ( (cloud == "amazon-ec2" || cloud == "amazon-vpc") && 
							DBRootStorageType == "external" && 
							config.moduleID() == moduleID)
					{
						if(!amazonVolumeCheck(id)) {
							//Set the alarm
							sendAlarm(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, SET);
							return API_FAILURE;
						}
					}
				}
			}
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on getStorageConfig: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on getStorageConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		}
	}

	return API_SUCCESS;
}


/******************************************************************************************
* @brief	processRestarted
*
* purpose:	Process Restarted, inform Process Mgr
*
*
******************************************************************************************/
int ProcessMonitor::processRestarted( std::string processName, bool manual)
{
	MonitorLog log;
//	MonitorConfig config;
//	ProcessMonitor aMonitor(config, log);
	Oam oam;
	ByteStream msg;

	log.writeLog(__LINE__, "Inform Process Mgr that process was restarted: " + processName, LOG_TYPE_DEBUG);

	int returnStatus = API_FAILURE;

	msg << (ByteStream::byte) PROCESSRESTART;
	msg << config.moduleName();
	msg << processName;
	msg << (ByteStream::byte) manual;

	try
	{
		MessageQueueClient mqRequest("ProcMgr");
		mqRequest.write(msg);
		mqRequest.shutdown();
		returnStatus = API_SUCCESS;
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueClient: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueClient: Caught unknown exception!", LOG_TYPE_ERROR);
	}

	return returnStatus;
}

/******************************************************************************************
* @brief	getDBRMdata
*
* purpose:	get DBRM Data from Process Mgr
*
*
******************************************************************************************/
int ProcessMonitor::getDBRMdata()
{
	MonitorLog log;

	Oam oam;
	ByteStream msg;

	log.writeLog(__LINE__, "getDBRMdata called", LOG_TYPE_DEBUG);

	int returnStatus = API_FAILURE;

	msg << (ByteStream::byte) GETDBRMDATA;
	msg << config.moduleName();

	try
	{
		MessageQueueClient mqRequest("ProcMgr");
		mqRequest.write(msg);

		ByteStream receivedMSG;
	
		struct timespec ts = { 30, 0 };

		//read message type
		try {
			receivedMSG = mqRequest.read(&ts);
		}
		catch (SocketClosed &ex) {
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: " + error, LOG_TYPE_ERROR);
			return returnStatus;
		}
		catch (...) {
			log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: Caught unknown exception!", LOG_TYPE_ERROR);
			return returnStatus;
		}

		if (receivedMSG.length() > 0) {

			string type;

			receivedMSG >> type;

			log.writeLog(__LINE__, type, LOG_TYPE_DEBUG);
			if ( type == "initial" ) {
				log.writeLog(__LINE__, "initial system, no dbrm files to send", LOG_TYPE_DEBUG);
				returnStatus = API_SUCCESS;
			}
			else
			{
				// files coming, read number of files
				try {
					receivedMSG = mqRequest.read(&ts);
				}
				catch (SocketClosed &ex) {
					string error = ex.what();
					log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: " + error, LOG_TYPE_ERROR);
					return returnStatus;
				}
				catch (...) {
					log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: Caught unknown exception!", LOG_TYPE_ERROR);
					return returnStatus;
				}
		
				if (receivedMSG.length() > 0) {

					ByteStream::byte numFiles;

					receivedMSG >> numFiles;
					log.writeLog(__LINE__, oam.itoa(numFiles), LOG_TYPE_DEBUG);

					bool journalFile = false;
	
					for ( int i = 0 ; i < numFiles ; i ++ )
					{
						string fileName;
	
						//read file name
						try {
							receivedMSG = mqRequest.read(&ts);
						}
						catch (SocketClosed &ex) {
							string error = ex.what();
							log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: " + error, LOG_TYPE_ERROR);
							return returnStatus;
						}
						catch (...) {
							log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: Caught unknown exception!", LOG_TYPE_ERROR);
							return returnStatus;
						}
				
						if (receivedMSG.length() > 0) {
							receivedMSG >> fileName;

							//check for journal file coming across
							string::size_type pos = fileName.find("journal",0);
							if (pos != string::npos)
								journalFile = true;

							//change file name location to temp file local
//							string::size_type pos1 = fileName.find("/dbrm",0);
//							pos = fileName.find("data1",0);
//							if (pos != string::npos)
//							{
//								string temp = fileName.substr(0,pos);
//								string temp1 = temp + "data" + fileName.substr(pos1,80);
//								fileName = temp1;
//							}

							boost::scoped_ptr<IDBDataFile> out(IDBDataFile::open(
														IDBPolicy::getType(fileName.c_str(),
														IDBPolicy::WRITEENG),
														fileName.c_str(), "w", 0));

							// read file data
							try {
								receivedMSG = mqRequest.read(&ts);
							}
							catch (SocketClosed &ex) {
								string error = ex.what();
								log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: " + error, LOG_TYPE_ERROR);
								return returnStatus;
							}
							catch (...) {
								log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: Caught unknown exception!", LOG_TYPE_ERROR);
								return returnStatus;
							}
					
							if (receivedMSG.length() > 0) {
								out->write(receivedMSG.buf(), receivedMSG.length());
								log.writeLog(__LINE__, fileName, LOG_TYPE_DEBUG);
								log.writeLog(__LINE__, oam.itoa(receivedMSG.length()), LOG_TYPE_DEBUG);
							}
							else
								log.writeLog(__LINE__, "ProcMgr Msg timeout on module", LOG_TYPE_ERROR);
						}
						else
							log.writeLog(__LINE__, "ProcMgr Msg timeout on module", LOG_TYPE_ERROR);
					}

					//create journal file if none come across
					if ( !journalFile)
					{
						string cmd = "touch " + startup::StartUp::installDir() + "/data1/systemFiles/dbrm/BRM_saves_journal";
						system(cmd.c_str());
					}

					returnStatus = oam::API_SUCCESS;
				}
				else
					log.writeLog(__LINE__, "ProcMgr Msg timeout on module", LOG_TYPE_ERROR);
			}
		}
		else
			log.writeLog(__LINE__, "ProcMgr Msg timeout on module", LOG_TYPE_ERROR);

		mqRequest.shutdown();
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueClient: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueClient: Caught unknown exception!", LOG_TYPE_ERROR);
	}

	return returnStatus;
}

/******************************************************************************************
* @brief	changeCrontab
*
* purpose:	update crontab to have logs archived at midnight
*
*
******************************************************************************************/
int ProcessMonitor::changeCrontab()
{
	MonitorLog log;
//	MonitorConfig config;
//	ProcessMonitor aMonitor(config, log);
	Oam oam;

	log.writeLog(__LINE__, "Update crontab to have daily logs saved at midnight", LOG_TYPE_DEBUG);

	string fileName = "/etc/crontab";

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return false;
	
	vector <string> lines;
	char line[200];
	string buf;
	string newLine = "58 23 * * * ";

	while (oldFile.getline(line, 200))
	{
		buf = line;

		string::size_type pos = buf.find("cron.daily",0);
		if (pos != string::npos)
		{
			pos = buf.find("root",0);
			newLine = newLine + buf.substr(pos, 200);

			log.writeLog(__LINE__, "Updated Crontab daily line = " + newLine, LOG_TYPE_DEBUG);

			buf = newLine;
		}
		//output to temp file
		lines.push_back(buf);
	}

	oldFile.close();
	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());	
	
	//create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	close(fd);

	// restart the service to make sure it running
	system("/etc/init.d/crond start > /dev/null 2>&1");
	system("/etc/init.d/crond reload > /dev/null 2>&1");

	return API_SUCCESS;
}

/******************************************************************************************
* @brief	changeTransactionLog
*
* purpose:	update TransactionLog based on system config "TransactionArchivePeriod"
*
*
******************************************************************************************/
int ProcessMonitor::changeTransactionLog()
{
	MonitorLog log;
//	MonitorConfig config;
//	ProcessMonitor aMonitor(config, log);
	Oam oam;

	string transactionArchivePeriod;
	try{
		oam.getSystemConfig("TransactionArchivePeriod", transactionArchivePeriod);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
		return API_FAILURE;
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		return API_FAILURE;
	}

	int tap = atoi(transactionArchivePeriod.c_str());
	if ( tap > 60 ) {
		tap = 60;
		transactionArchivePeriod = "60";
	}
	if ( tap <= 0 ) {
		tap = 10;
		transactionArchivePeriod = "10";
	}
		 
	log.writeLog(__LINE__, "Configure Tranasction Log at " + transactionArchivePeriod + " minutes", LOG_TYPE_DEBUG);

	string fileName = "/etc/cron.d/transactionLog";

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return false;
	
	vector <string> lines;
	char line[200];
	string buf;
	string newLine = "00";

	while (oldFile.getline(line, 200))
	{
		buf = line;

		string::size_type pos = buf.find("transactionLogArchiver.sh",0);
		if (pos != string::npos)
		{
			int minutes = tap;
			for ( ; minutes < 60 ; )
			{
				newLine = newLine + "," + oam.itoa(minutes);
				minutes = minutes + tap;
			}

			pos = buf.find(" *",0);
			newLine = newLine + buf.substr(pos, 200);

			log.writeLog(__LINE__, "Updated TransactionLog line = " + newLine, LOG_TYPE_DEBUG);

			buf = newLine;
		}
		//output to temp file
		lines.push_back(buf);
	}

	oldFile.close();
	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());	
	
	//create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	close(fd);

	// restart the service to make sure it running
	system("/etc/init.d/crond start > /dev/null 2>&1");
	system("/etc/init.d/crond reload > /dev/null 2>&1");

	return API_SUCCESS;
}

/******************************************************************************************
* @brief	setup Tranaction Logging
*
* purpose:	Active or Deactive Transaction logging
*
******************************************************************************************/
void ProcessMonitor::setTransactionLog(bool action)
{

	if (action) {
		//set
		log.writeLog(__LINE__, "setTransactionLog: setup ", LOG_TYPE_DEBUG);
		changeTransactionLog();

		string cmd = "cp " + startup::StartUp::installDir() + "/bin/transactionLog /etc/cron.d/.";
		system(cmd.c_str());
		log.writeLog(__LINE__, "Successful copied transactionLog script", LOG_TYPE_DEBUG);

		system("chmod 644 /etc/cron.d/transactionLog");
		log.writeLog(__LINE__, "Successful chmod on transactionLog", LOG_TYPE_DEBUG);
	}
	else
	{
		//clear
		log.writeLog(__LINE__, "setTransactionLog: clear", LOG_TYPE_DEBUG);
		system("rm -f /etc/cron.d/transactionLog");
		log.writeLog(__LINE__, "Successful deleted transactionLog script", LOG_TYPE_DEBUG);
	}

	// restart the crond service to make sure it running
	log.writeLog(__LINE__, "Start and reload crond", LOG_TYPE_DEBUG);
	system("/etc/init.d/crond start > /dev/null 2>&1");
	system("/etc/init.d/crond reload > /dev/null 2>&1");
}

/******************************************************************************************
* @brief	runStartupTest
*
* purpose:	Runs DB sanity test
*
*
******************************************************************************************/
int ProcessMonitor::runStartupTest()
{
	//ProcMon log file 
	MonitorLog log;
//	MonitorConfig config;
//	ProcessMonitor aMonitor(config, log);
	Oam oam;

	//skip if module is DISABLED
	int opState;
	bool degraded;
	oam.getModuleStatus(config.moduleName(), opState, degraded);
	if (geteuid() != 0 || opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
		return oam::API_SUCCESS;

	//run startup test script
	string logdir("/var/log/Calpont");
	if (access(logdir.c_str(), W_OK) != 0) logdir = "/tmp";
	string cmd = startup::StartUp::installDir() + "/bin/startupTests.sh > " + logdir + "/startupTests.log1 2>&1";
	system(cmd.c_str());

	cmd = logdir + "/startupTests.log1";

	bool fail = false;

	if (oam.checkLogStatus(cmd, "OK")) {
		log.writeLog(__LINE__, "startupTests passed", LOG_TYPE_DEBUG);
	}
	else
	{
		log.writeLog(__LINE__, "startupTests failed", LOG_TYPE_CRITICAL);
		fail = true;
	}
	
	if (!fail)
	{
		log.writeLog(__LINE__, "runStartupTest passed", LOG_TYPE_DEBUG);
		//Clear the alarm 
		sendAlarm(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, CLEAR);
		return oam::API_SUCCESS;
	}
	else
	{
		log.writeLog(__LINE__, "ERROR: runStartupTest failed", LOG_TYPE_CRITICAL);
		//Set the alarm 
		sendAlarm(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, SET);

		//setModule status to failed
		try{
			oam.setModuleStatus(config.moduleName(), oam::FAILED);
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: Caught unknown exception!", LOG_TYPE_ERROR);
		}
		return oam::API_FAILURE;
	}
}

/******************************************************************************************
* @brief	runHDFSTest
*
* purpose:	Runs HDFS sanity test
*
*
******************************************************************************************/
int ProcessMonitor::runHDFSTest()
{
	//ProcMon log file 
	MonitorLog log;
//	MonitorConfig config;
//	ProcessMonitor aMonitor(config, log);
	Oam oam;
	bool fail = false;

	string logdir("/var/log/Calpont");
	if (access(logdir.c_str(), W_OK) != 0) logdir = "/tmp";
	string hdfslog = logdir + "/hdfsCheck.log1";

	//run hadoop test
	string DataFilePlugin;
	oam.getSystemConfig("DataFilePlugin", DataFilePlugin);

	ifstream File (DataFilePlugin.c_str());
	if (!File) {
		log.writeLog(__LINE__, "Error: Hadoop Datafile Plugin File (" + DataFilePlugin + ") doesn't exist", LOG_TYPE_CRITICAL);
		fail = true;
	}
	else
	{
		// retry since infinidb can startup before hadoop is full up
		int retry = 0;
		for (  ; retry < 12 ; retry++ )
		{
			string cmd = startup::StartUp::installDir() + "/bin/hdfsCheck " + DataFilePlugin +  " > " + hdfslog + " 2>&1";
			system(cmd.c_str());
			if (oam.checkLogStatus(hdfslog, "All HDFS checks passed!")) {
				log.writeLog(__LINE__, "hdfsCheck passed", LOG_TYPE_DEBUG);
				break;
			}
			else
			{
				log.writeLog(__LINE__, "hdfsCheck Failed, wait and try again", LOG_TYPE_DEBUG);
				sleep(10);
			}
		}

		if ( retry >= 12 )
		{
			log.writeLog(__LINE__, "hdfsCheck Failed, check " + hdfslog, LOG_TYPE_CRITICAL);
			fail = true;
		}
	}
	
	if (!fail)
	{
		//Clear the alarm 
		sendAlarm(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, CLEAR);
		return oam::API_SUCCESS;
	}
	else
	{
		//Set the alarm 
		sendAlarm(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, SET);

		//setModule status to failed
		try{
			oam.setModuleStatus(config.moduleName(), oam::FAILED);
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on setModuleStatus: Caught unknown exception!", LOG_TYPE_ERROR);
		}

		return oam::API_FAILURE;
	}
}


/******************************************************************************************
* @brief	updateConfigFile
*
* purpose:	Update local Calpont Config File
*
******************************************************************************************/
int ProcessMonitor::updateConfigFile(messageqcpp::ByteStream msg)
{
	Config* sysConfig = Config::makeConfig();
	sysConfig->writeConfigFile(msg);

	return oam::API_SUCCESS;
}



/******************************************************************************************
* @brief	sendMsgProcMon1
*
* purpose:	Sends a Msg to ProcMon
*
******************************************************************************************/
std::string ProcessMonitor::sendMsgProcMon1( std::string module, ByteStream msg, int requestID )
{
	string msgPort = module + "_ProcessMonitor";
	string returnStatus = "FAILED";

	// do a ping test to determine a quick failure
	Config* sysConfig = Config::makeConfig();

	string IPAddr = sysConfig->getConfig(msgPort, "IPAddr");

	string cmdLine = "ping ";
	string cmdOption = " -c 1 -w 5 >> /dev/null";
	string cmd = cmdLine + IPAddr + cmdOption;
	if ( system(cmd.c_str()) != 0 ) {
		//ping failure
		log.writeLog(__LINE__, "sendMsgProcMon ping failure", LOG_TYPE_ERROR);
		return returnStatus;
	}

	try
	{
		MessageQueueClient mqRequest(msgPort);
		mqRequest.write(msg);
	
		// wait 30 seconds for response
		ByteStream::byte returnACK;
		ByteStream::byte returnRequestID;
		string requestStatus;
		ByteStream receivedMSG;
	
		struct timespec ts = { 30, 0 };
		try {
			receivedMSG = mqRequest.read(&ts);
		}
		catch (SocketClosed &ex) {
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: " + error, LOG_TYPE_ERROR);
			return returnStatus;
		}
		catch (...) {
			log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: Caught unknown exception!", LOG_TYPE_ERROR);
			return returnStatus;
		}

		if (receivedMSG.length() > 0) {
			receivedMSG >> returnACK;
			receivedMSG >> returnRequestID;
			receivedMSG >> requestStatus;
	
			if ( returnACK == oam::ACK &&  returnRequestID == requestID) {
				// ACK for this request
				returnStatus = requestStatus;
			}
			else
				log.writeLog(__LINE__, "sendMsgProcMon: message mismatch ", LOG_TYPE_ERROR);	
		}
		else
			log.writeLog(__LINE__, "sendMsgProcMon: ProcMon Msg timeout on module " + module, LOG_TYPE_ERROR);

		mqRequest.shutdown();
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueClient: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueClient: Caught unknown exception!", LOG_TYPE_ERROR);
	}

	return returnStatus;
}

/******************************************************************************************
* @brief	checkProcessFailover
*
* purpose:	check if process failover is needed due to a process outage
*
******************************************************************************************/
void ProcessMonitor::checkProcessFailover( std::string processName)
{
	Oam oam;

	//force failover on certain processes
	if ( processName == "DDLProc" ||
		processName == "DMLProc" ) {
			log.writeLog(__LINE__, "checkProcessFailover: process failover, process outage of " + processName, LOG_TYPE_CRITICAL);

		try
		{
			SystemProcessStatus systemprocessstatus;
			oam.getProcessStatus(systemprocessstatus);
	
			for( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
			{
				if ( systemprocessstatus.processstatus[i].ProcessName == processName  &&
						systemprocessstatus.processstatus[i].Module != config.moduleName() ) {
					//make sure it matches module type
					string procModuleType = systemprocessstatus.processstatus[i].Module.substr(0,MAX_MODULE_TYPE_SIZE);
					if ( config.moduleType() != procModuleType )
						continue;
					if ( systemprocessstatus.processstatus[i].ProcessOpState == oam::COLD_STANDBY || 
						systemprocessstatus.processstatus[i].ProcessOpState == oam::AUTO_OFFLINE ||
						systemprocessstatus.processstatus[i].ProcessOpState == oam::FAILED ) {
						// found a AVAILABLE mate, start it
						log.writeLog(__LINE__, "start process on module " + systemprocessstatus.processstatus[i].Module, LOG_TYPE_DEBUG);

						try {
							oam.setSystemConfig("PrimaryUMModuleName", systemprocessstatus.processstatus[i].Module);

							//distribute config file
							oam.distributeConfigFile("system");
							sleep(1);
						}
						catch(...) {}

						try
						{
							oam.startProcess(systemprocessstatus.processstatus[i].Module, processName, FORCEFUL, ACK_YES);
							log.writeLog(__LINE__, "success start process on module " + systemprocessstatus.processstatus[i].Module, LOG_TYPE_DEBUG);
						}
						catch (exception& e)
						{
							log.writeLog(__LINE__, "failed start process on module " + systemprocessstatus.processstatus[i].Module, LOG_TYPE_ERROR);
						}
						break;
					}
				}
			}
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
		}
	}

	return;

}

/******************************************************************************************
* @brief	runUpgrade
*
* purpose:	run upgrade script
*
******************************************************************************************/
int ProcessMonitor::runUpgrade(std::string mysqlpw)
{
	Oam oam;

	for ( int i = 0 ; i < 10 ; i++ )
	{
		//run upgrade script
		string cmd = startup::StartUp::installDir() + "/bin/upgrade-infinidb.sh doupgrade --password=" +
			mysqlpw + " --installdir=" +  startup::StartUp::installDir() + "  > " + "/tmp/upgrade-infinidb.log 2>&1";
		system(cmd.c_str());

		cmd = "/tmp/upgrade-infinidb.log";
		if (oam.checkLogStatus(cmd, "OK")) {
			log.writeLog(__LINE__, "upgrade-infinidb.sh: Successful return", LOG_TYPE_DEBUG);
			return oam::API_SUCCESS;
		}
		else {
			if (oam.checkLogStatus(cmd, "ERROR 1045") ) {
				log.writeLog(__LINE__, "upgrade-infinidb.sh: Missing Password error, return success", LOG_TYPE_DEBUG);
				return oam::API_SUCCESS;
			}

			log.writeLog(__LINE__, "upgrade-infinidb.sh: Error return, check log /tmp/upgrade-status.log", LOG_TYPE_ERROR);
			//restart mysqld and retry
			try {
				oam.actionMysqlCalpont(MYSQL_RESTART);
			}
			catch(...)
			{}
			sleep(1);
		}
	}
	return oam::API_FAILURE;
}


/******************************************************************************************
* @brief	changeMyCnf
*
* purpose:	Change my.cnf
*
******************************************************************************************/
int ProcessMonitor::changeMyCnf(std::string type)
{
	Oam oam;

	log.writeLog(__LINE__, "changeMyCnf function called for " + type, LOG_TYPE_DEBUG);

	string mycnfFile = startup::StartUp::installDir() + "/mysql/my.cnf";
	ifstream file (mycnfFile.c_str());
	if (!file) {
		log.writeLog(__LINE__, "changeMyCnf - my.cnf file not found: " + mycnfFile, LOG_TYPE_CRITICAL);
		return oam::API_FAILURE;
	}

	if ( type == "master" )
	{
		// set master replication entries
		vector <string> lines;
		char line[200];
		string buf;
		while (file.getline(line, 200))
		{
			buf = line;
			string::size_type pos = buf.find("server-id =",0);
			if ( pos != string::npos ) {
				buf = "server-id = 1";
			}

			pos = buf.find("# log-bin=mysql-bin",0);
			if ( pos != string::npos ) {
				buf = "log-bin=mysql-bin";
			}

			pos = buf.find("infinidb_local_query=1",0);
			if ( pos != string::npos && pos == 0) {
				buf = "# infinidb_local_query=1";
			}

			//output to temp file
			lines.push_back(buf);
		}

		file.close();
		unlink (mycnfFile.c_str());
		ofstream newFile (mycnfFile.c_str());	
		
		//create new file
		int fd = open(mycnfFile.c_str(), O_RDWR|O_CREAT, 0666);
		
		copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
		newFile.close();
		
		close(fd);
	}

	if ( type == "slave" )
	{
		//get slave id based on ExeMgrx setup
		string slaveID = "0";
		string slaveModuleName = config.moduleName();
		for ( int id = 1 ; ; id++ )
		{
			string Section = "ExeMgr" + oam.itoa(id);

			string moduleName;

			try {
				Config* sysConfig = Config::makeConfig();
				moduleName = sysConfig->getConfig(Section, "Module");

				if ( moduleName == slaveModuleName )
				{
					slaveID = oam.itoa(id);

					// if slave ID from above is 1, then it means this is a former Original master and use the ID of the current Master
					if ( slaveID == "1" ) {
						string PrimaryUMModuleName;
						oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);

						for ( int mid = 1 ; ; mid++ )
						{
							string Section = "ExeMgr" + oam.itoa(mid);
				
							string moduleName;
				
							try {
								Config* sysConfig = Config::makeConfig();
								moduleName = sysConfig->getConfig(Section, "Module");
				
								if ( moduleName == PrimaryUMModuleName )
								{
									slaveID = oam.itoa(mid);
									break;
								}
							}
							catch (...) {}
						}
					}
					break;
				}
			}
			catch (...) {}
		}

		if ( slaveID == "0" )
		{
			log.writeLog(__LINE__, "changeMyCnf: ExeMgr for local module doesn't exist", LOG_TYPE_ERROR);
			return oam::API_FAILURE;
		}

		// get local host name
		string HOSTNAME = "localhost";
		try
		{
			ModuleConfig moduleconfig;
			oam.getSystemConfig(config.moduleName(), moduleconfig);
			HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
			HOSTNAME = (*pt1).HostName;
		}
		catch(...)
		{}

		char* p= getenv("HOSTNAME");
		if (p && *p)
			HOSTNAME = p;

		// set slave replication entries
		vector <string> lines;
		char line[200];
		string buf;
		while (file.getline(line, 200))
		{
			buf = line;
			string::size_type pos = buf.find("server-id =",0);
			if ( pos != string::npos ) {
				buf = "server-id = " + slaveID;
			}

			pos = buf.find("#relay-log=HOSTNAME-relay-bin",0);
			if ( pos != string::npos ) {
				buf = "relay-log=" + HOSTNAME + "-relay-bin";
			}

			// set local query flag if on pm
			if ( config.moduleType() == "pm" )
			{
				pos = buf.find("# infinidb_local_query=1",0);
				if ( pos != string::npos ) {
					buf = "infinidb_local_query=1";
				}
			}

			pos = buf.find("log-bin=mysql-bin",0);
			if ( pos != string::npos && pos == 0 ) {
				buf = "# log-bin=mysql-bin";
			}

			//output to temp file
			lines.push_back(buf);
		}

		file.close();
		unlink (mycnfFile.c_str());
		ofstream newFile (mycnfFile.c_str());	
		
		//create new file
		int fd = open(mycnfFile.c_str(), O_RDWR|O_CREAT, 0666);
		
		copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
		newFile.close();
		
		close(fd);
	}

	// restart mysql
	try {
		oam.actionMysqlCalpont(MYSQL_RESTART);
	}
	catch(...)
	{}

	return oam::API_SUCCESS;
}

/******************************************************************************************
* @brief	runMasterRep
*
* purpose:	run Master Replication script
*
******************************************************************************************/
int ProcessMonitor::runMasterRep(std::string& mysqlpw, std::string& masterLogFile, std::string& masterLogPos)
{
	Oam oam;

	log.writeLog(__LINE__, "runMasterRep function called", LOG_TYPE_DEBUG);

	SystemModuleTypeConfig systemModuleTypeConfig;
	try {
		oam.getSystemConfig(systemModuleTypeConfig);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
	}

	// create user for each module by ip address
	for ( unsigned int i = 0 ; i < systemModuleTypeConfig.moduletypeconfig.size(); i++)
	{
		int moduleCount = systemModuleTypeConfig.moduletypeconfig[i].ModuleCount;
		if( moduleCount == 0)
			continue;

		DeviceNetworkList::iterator pt = systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
		for( ; pt != systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
		{
			string moduleName =  (*pt).DeviceName;

			HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
			for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++ )
			{
				string ipAddr = (*pt1).IPAddr;

				string logFile = "/tmp/master-rep-infinidb-" + moduleName + ".log";
				string cmd = startup::StartUp::installDir() + "/bin/master-rep-infinidb.sh --password=" +
					mysqlpw + " --installdir=" + startup::StartUp::installDir() + " --hostIP=" + ipAddr + "  > " + logFile + " 2>&1";
				system(cmd.c_str());
		
				if (oam.checkLogStatus(logFile, "OK"))
					log.writeLog(__LINE__, "master-rep-infinidb.sh: Successful return for node " + moduleName, LOG_TYPE_DEBUG);
				else 
				{
					if (oam.checkLogStatus(logFile, "ERROR 1045") ) {
						log.writeLog(__LINE__, "master-rep-infinidb.sh: Missing Password error, return success", LOG_TYPE_DEBUG);
						return oam::API_SUCCESS;
					}
					else
					{
						log.writeLog(__LINE__, "master-rep-infinidb.sh: Error return, check log " + logFile, LOG_TYPE_ERROR);
						return oam::API_FAILURE;
					}
				}
			}
		}
	}

	// go parse out the MASTER_LOG_FILE and MASTER_LOG_POS
	// this is what the output will look like
	//
	//	SHOW MASTER STATUS
	//	File	Position	Binlog_Do_DB	Binlog_Ignore_DB
	//	mysql-bin.000006	2921
	// 
	// in log - /tmp/show-master-status.log

	ifstream file ("/tmp/show-master-status.log");
	if (!file) {
		log.writeLog(__LINE__, "runMasterRep - show master status log file doesn't exist - /tmp/show-master-status.log", LOG_TYPE_CRITICAL);
		return oam::API_FAILURE;
	}
	else
	{
		char line[200];
		string buf;
		while (file.getline(line, 200))
		{
			buf = line;
			string::size_type pos = buf.find("mysql-bin",0);
			if ( pos != string::npos ) {
				string::size_type pos1 = buf.find("\t",pos);
				if ( pos1 != string::npos ) {
					string masterlogfile = buf.substr(pos,pos1-pos);

					//strip trailing spaces
					string::size_type lead = masterlogfile.find_first_of(" ");
					masterLogFile = masterlogfile.substr( 0, lead);

					string masterlogpos = buf.substr(pos1,80);

					//strip off leading tab masterlogpos
					lead=masterlogpos.find_first_not_of("\t");
					masterlogpos = masterlogpos.substr( lead, masterlogpos.length()-lead);

					//string trailing spaces
					lead = masterlogpos.find_first_of(" ");
					masterLogPos = masterlogpos.substr( 0, lead);

					log.writeLog(__LINE__, "runMasterRep: masterlogfile=" + masterLogFile + ", masterlogpos=" + masterLogPos, LOG_TYPE_DEBUG);
					file.close();
					return oam::API_SUCCESS;
				}
			}
		}

		file.close();
	}

	log.writeLog(__LINE__, "runMasterRep - 'mysql-bin not found in log file - /tmp/show-master-status.log", LOG_TYPE_CRITICAL);
	return oam::API_FAILURE;
}

/******************************************************************************************
* @brief	runSlaveRep
*
* purpose:	run Slave Replication script
*
******************************************************************************************/
int ProcessMonitor::runSlaveRep(std::string& mysqlpw, std::string& masterLogFile, std::string& masterLogPos)
{
	Oam oam;

	log.writeLog(__LINE__, "runSlaveRep function called", LOG_TYPE_DEBUG);

	// get master replicaion module IP Address
	string PrimaryUMModuleName;
	oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);

	string masterIPAddress;
	try
	{
		ModuleConfig moduleconfig;
		oam.getSystemConfig(PrimaryUMModuleName, moduleconfig);
		HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
		masterIPAddress = (*pt1).IPAddr;
	}
	catch(...)
	{}

	string cmd = startup::StartUp::installDir() + "/bin/slave-rep-infinidb.sh --password=" +
		mysqlpw + " --installdir=" + startup::StartUp::installDir() + " --masteripaddr=" + masterIPAddress + " --masterlogfile=" + masterLogFile  + " --masterlogpos=" + masterLogPos + "  >   /tmp/slave-rep-infinidb.log 2>&1";
	system(cmd.c_str());

	cmd = "/tmp/slave-rep-infinidb.log";
	if (oam.checkLogStatus(cmd, "OK")) {
		log.writeLog(__LINE__, "slave-rep-infinidb.sh: Successful return", LOG_TYPE_DEBUG);
		return oam::API_SUCCESS;
	}
	else {
		if (oam.checkLogStatus(cmd, "ERROR 1045") ) {
			log.writeLog(__LINE__, "slave-rep-infinidb.sh: Missing Password error, return success", LOG_TYPE_DEBUG);
			return oam::API_SUCCESS;
		}

		log.writeLog(__LINE__, "slave-rep-infinidb.sh: Error return, check log /tmp/slave-rep-infinidb.log", LOG_TYPE_ERROR);
		return oam::API_FAILURE;
	}

	return oam::API_FAILURE;
}

/******************************************************************************************
* @brief	runMasterDist
*
* purpose:	run Master DB Distribution
*
******************************************************************************************/
int ProcessMonitor::runMasterDist(std::string& password, std::string& slaveModule)
{
	Oam oam;

	log.writeLog(__LINE__, "runMasterDist function called", LOG_TYPE_DEBUG);

	SystemModuleTypeConfig systemModuleTypeConfig;
	try {
		oam.getSystemConfig(systemModuleTypeConfig);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
	}

	if ( slaveModule == "all" )
	{
		// Distrubuted MySQL Front-end DB to Slave Modules
		for ( unsigned int i = 0 ; i < systemModuleTypeConfig.moduletypeconfig.size(); i++)
		{
			int moduleCount = systemModuleTypeConfig.moduletypeconfig[i].ModuleCount;
			if( moduleCount == 0)
				continue;
	
			string moduleType = systemModuleTypeConfig.moduletypeconfig[i].ModuleType;
	
			if ( (PMwithUM == "n") && (moduleType == "pm") && ( config.ServerInstallType() != oam::INSTALL_COMBINE_DM_UM_PM) )
				continue;
	
			DeviceNetworkList::iterator pt = systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for( ; pt != systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
			{
				string moduleName =  (*pt).DeviceName;
	
				//skip if local master mode
				if ( moduleName == config.moduleName() )
					continue;
	
				HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
				for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++ )
				{
					string ipAddr = (*pt1).IPAddr;
	
					string cmd = startup::StartUp::installDir() + "/bin/rsync.sh " + ipAddr + " " + password + " 1 > /tmp/master-dist_" + moduleName + ".log";
					int ret = system(cmd.c_str());
				
					if ( WEXITSTATUS(ret) == 0 )
						log.writeLog(__LINE__, "runMasterDist: Success rsync to module: " + moduleName, LOG_TYPE_DEBUG);
					else
						log.writeLog(__LINE__, "runMasterDist: Failure rsync to module: " + moduleName, LOG_TYPE_ERROR);
				}
			}
		}
	}
	else
	{
		// get slave IP address
		ModuleConfig moduleconfig;
		oam.getSystemConfig(slaveModule, moduleconfig);
		HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
		string ipAddr = (*pt1).IPAddr;

		string cmd = startup::StartUp::installDir() + "/bin/rsync.sh " + ipAddr + " " + password + " 1 > /tmp/master-dist_" + slaveModule + ".log";
		int ret = system(cmd.c_str());
	
		if ( WEXITSTATUS(ret) == 0 )
			log.writeLog(__LINE__, "runMasterDist: Success rsync to module: " + slaveModule, LOG_TYPE_DEBUG);
		else
			log.writeLog(__LINE__, "runMasterDist: Failure rsync to module: " + slaveModule, LOG_TYPE_ERROR);
	}

	return oam::API_SUCCESS;
}


/******************************************************************************************
* @brief	amazonIPCheck
*
* purpose:	check and setups Amazon EC2 IP Addresses
*
******************************************************************************************/
bool ProcessMonitor::amazonIPCheck()
{
	MonitorLog log;
	Oam oam;

	log.writeLog(__LINE__, "amazonIPCheck function called", LOG_TYPE_DEBUG);

	// delete description file so it will create a new one
	unlink("/tmp/describeInstance.txt");

	//
	// Get Module Info
	//
	SystemModuleTypeConfig systemModuleTypeConfig;

	try{
		oam.getSystemConfig(systemModuleTypeConfig);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
	}

	//get Elastic IP Address count
	int AmazonElasticIPCount = 0;
	try{
		oam.getSystemConfig("AmazonElasticIPCount", AmazonElasticIPCount);
	}
	catch(...) {
		AmazonElasticIPCount = 0;
	}

	ModuleTypeConfig moduletypeconfig;

	//get module/instance IDs
	for ( unsigned int i = 0 ; i < systemModuleTypeConfig.moduletypeconfig.size(); i++)
	{
		int moduleCount = systemModuleTypeConfig.moduletypeconfig[i].ModuleCount;
		if ( moduleCount == 0 )
			// skip of no modules configured
			continue;

		DeviceNetworkList::iterator pt = systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
		for( ; pt != systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
		{
			DeviceNetworkConfig devicenetworkconfig;
			HostConfig hostconfig;

			devicenetworkconfig.DeviceName = (*pt).DeviceName;

			HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
			for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++ )
			{
				hostconfig.IPAddr = (*pt1).IPAddr;
				hostconfig.HostName = (*pt1).HostName;
				devicenetworkconfig.hostConfigList.push_back(hostconfig);
			}
			
			moduletypeconfig.ModuleNetworkList.push_back(devicenetworkconfig);
		}
	}

	// now loop and wait for 5 minutes for all configured Instances to be running
	// like after a reboot
	bool startFail = false;
	for ( int time = 0 ; time < 30 ; time++ )
	{
		startFail = false;
		DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();
		for ( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
		{
			string moduleName = (*pt).DeviceName;

			// get all ips if parent oam
			// get just parent and local if not parent oam
			if ( config.moduleName() == config.OAMParentName() ||
					moduleName == config.moduleName() ||
					moduleName == config.OAMParentName() )
			{
				HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
				for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
				{
					string IPAddr = (*pt1).IPAddr;
					string instanceID = (*pt1).HostName;
	
					log.writeLog(__LINE__, "getEC2InstanceIpAddress called to get status for Module '" + moduleName + "' / Instance " + instanceID, LOG_TYPE_DEBUG);
					string currentIPAddr = oam.getEC2InstanceIpAddress(instanceID);
	
					if (currentIPAddr == "stopped") {
						log.writeLog(__LINE__, "Module '" + moduleName + "' / Instance '" + instanceID + "' not running", LOG_TYPE_WARNING);
						startFail = true;
					}
					else
					{
						if (currentIPAddr == "terminated") {
							log.writeLog(__LINE__, "Module '" + moduleName + "' / Instance '" + instanceID + "' has no Private IP Address Assigned, system failed to start", LOG_TYPE_CRITICAL);
							startFail = true;
							break;
						}
						else
						{
							if ( currentIPAddr != IPAddr ) {
								log.writeLog(__LINE__, "Module is Running: '" + moduleName + "' / Instance '" + instanceID + "' current IP being reconfigured in Calpont.xml. old = " + IPAddr + ", new = " + currentIPAddr, LOG_TYPE_DEBUG);
		
								// update the Calpont.xml with the new IP Address
								string cmd = "sed -i s/" + IPAddr + "/" + currentIPAddr + "/g /usr/local/Calpont/etc/Calpont.xml";
								system(cmd.c_str());
							}
							else
								log.writeLog(__LINE__, "Module is Running: '" + moduleName + "' / Instance '" + instanceID + "' current IP didn't change.", LOG_TYPE_DEBUG);
						}
					}

					//set Elastic IP Address, if configured
					if (AmazonElasticIPCount > 0)
					{
						bool found = false;
						int id = 1;
						for (  ; id < AmazonElasticIPCount+1 ; id++ )
						{
							string AmazonElasticModule = "AmazonElasticModule" + oam.itoa(id);
							string ELmoduleName;
							string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);
							string ELIPaddress;
							try{
								oam.getSystemConfig(AmazonElasticModule, ELmoduleName);
								oam.getSystemConfig(AmazonElasticIPAddr, ELIPaddress);
							}
							catch(...) {}
			
							if ( ELmoduleName == moduleName )
							{
								found = true;
								try{
									oam.assignElasticIP(instanceID, ELIPaddress);
									log.writeLog(__LINE__, "Assign Elastic IP Address : '" + moduleName + "' / '" + ELIPaddress, LOG_TYPE_DEBUG);
								}
								catch(...) {
									log.writeLog(__LINE__, "Assign Elastic IP Address failed : '" + moduleName + "' / '" + ELIPaddress, LOG_TYPE_ERROR);
									break;
								}

								break;
							}

							if(found)
								break;
						}
					}
				}
			}
		}

		//continue when no instances are stopped
		if (!startFail)
			break;

		sleep(10);
	}

	//check if an instance is stopped, exit out...
	if (startFail) {
		log.writeLog(__LINE__, "A configured Instance isn't running. Check warning log", LOG_TYPE_CRITICAL);
	}

	log.writeLog(__LINE__, "amazonIPCheck function successfully completed", LOG_TYPE_DEBUG);

	return true;

}

/******************************************************************************************
* @brief	amazonVolumeCheck
*
* purpose:	check and setups Amazon EC2 Volume mounts
*
******************************************************************************************/
bool ProcessMonitor::amazonVolumeCheck(int dbrootID)
{
	MonitorLog log;
	Oam oam;

	if ( config.moduleType() == "um")
	{
		log.writeLog(__LINE__, "amazonVolumeCheck function called for User Module", LOG_TYPE_DEBUG);

		string volumeNameID = "UMVolumeName" + oam.itoa(config.moduleID());
		string volumeName = oam::UnassignedName;
		string deviceNameID = "UMVolumeDeviceName" + oam.itoa(config.moduleID());
		string deviceName = oam::UnassignedName;
		try {
			oam.getSystemConfig( volumeNameID, volumeName);
			oam.getSystemConfig( deviceNameID, deviceName);
		}
		catch(...)
		{}
	
		if ( volumeName.empty() || volumeName == oam::UnassignedName ) {
			log.writeLog(__LINE__, "amazonVolumeCheck function exiting, no volume assigned ", LOG_TYPE_WARNING);
			return false;
		}
	
		string status = oam.getEC2VolumeStatus(volumeName);
		if ( status == "attached" ) {
			string cmd = "mount " + deviceName + " " + startup::StartUp::installDir() + "/mysql/db -t ext2 -o defaults > /dev/null";
			system(cmd.c_str());
			log.writeLog(__LINE__, "mount cmd: " + cmd, LOG_TYPE_DEBUG);

			log.writeLog(__LINE__, "amazonVolumeCheck function successfully completed, volume attached: " + volumeName, LOG_TYPE_DEBUG);
			return true;
		}
	
		if ( status != "available" ) {
			log.writeLog(__LINE__, "amazonVolumeCheck function failed, volume not attached and not available: " + volumeName, LOG_TYPE_CRITICAL);
			return false;
		}
		else
		{
			//get Module HostName / InstanceName
			string instanceName;
			try
			{
				ModuleConfig moduleconfig;
				oam.getSystemConfig(config.moduleName(), moduleconfig);
				HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
				instanceName = (*pt1).HostName;
			}
			catch(...)
			{}
	
			if (oam.attachEC2Volume(volumeName, deviceName, instanceName)) {
				string cmd = "mount " + deviceName + " " + startup::StartUp::installDir() + "/mysql/db -t ext2 -o defaults > /dev/null";
				system(cmd.c_str());
				log.writeLog(__LINE__, "mount cmd: " + cmd, LOG_TYPE_DEBUG);
				return true;
			}
			else
			{
				log.writeLog(__LINE__, "amazonVolumeCheck function failed, volume failed to attached: " + volumeName, LOG_TYPE_CRITICAL);
				return false;
			}
		}
	}
	else
	{
		log.writeLog(__LINE__, "amazonVolumeCheck function called for dbroot" + oam.itoa(dbrootID), LOG_TYPE_DEBUG);
	
		string volumeNameID = "PMVolumeName" + oam.itoa(dbrootID);
		string volumeName = oam::UnassignedName;
		string deviceNameID = "PMVolumeDeviceName" + oam.itoa(dbrootID);
		string deviceName = oam::UnassignedName;
		try {
			oam.getSystemConfig( volumeNameID, volumeName);
			oam.getSystemConfig( deviceNameID, deviceName);
		}
		catch(...)
		{}
	
		if ( volumeName.empty() || volumeName == oam::UnassignedName ) {
			log.writeLog(__LINE__, "amazonVolumeCheck function exiting, no volume assigned to dbroot " + oam.itoa(dbrootID), LOG_TYPE_WARNING);
			return false;
		}
	
		string status = oam.getEC2VolumeStatus(volumeName);
		if ( status == "attached" ) {
			log.writeLog(__LINE__, "amazonVolumeCheck function successfully completed, volume attached: " + volumeName, LOG_TYPE_DEBUG);
			return true;
		}
	
		if ( status != "available" ) {
			log.writeLog(__LINE__, "amazonVolumeCheck function failed, volume not attached and not available: " + volumeName, LOG_TYPE_CRITICAL);
			return false;
		}
		else
		{
			//get Module HostName / InstanceName
			string instanceName;
			try
			{
				ModuleConfig moduleconfig;
				oam.getSystemConfig(config.moduleName(), moduleconfig);
				HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
				instanceName = (*pt1).HostName;
			}
			catch(...)
			{}
	
			if (oam.attachEC2Volume(volumeName, deviceName, instanceName)) {
				string cmd = "mount " + startup::StartUp::installDir() + "/data" + oam.itoa(dbrootID) + " > /dev/null";
				system(cmd.c_str());
				return true;
			}
			else
			{
				log.writeLog(__LINE__, "amazonVolumeCheck function failed, volume failed to attached: " + volumeName, LOG_TYPE_CRITICAL);
				return false;
			}
		}
	}

	log.writeLog(__LINE__, "amazonVolumeCheck function successfully completed", LOG_TYPE_DEBUG);

	return true;

}

/******************************************************************************************
* @brief	unmountExtraDBroots
*
* purpose:	unmount Extra DBroots which were left mounted during a move
*
*
******************************************************************************************/
void ProcessMonitor::unmountExtraDBroots()
{
	MonitorLog log;
	ModuleConfig moduleconfig;
	Oam oam;

	string DBRootStorageType = "internal";

	try{
		oam.getSystemConfig("DBRootStorageType", DBRootStorageType);

		if ( DBRootStorageType == "hdfs" ||
			( DBRootStorageType == "internal" && GlusterConfig == "n") )
			return;
	}
	catch(...) {}

	if (GlusterConfig == "y")
		return;

	try
	{
		systemStorageInfo_t t;
		t = oam.getStorageConfig();

		if ( boost::get<1>(t) == 0 ) {
			return;
		}

		DeviceDBRootList moduledbrootlist = boost::get<2>(t);

		//Flush the cache
		cacheutils::flushPrimProcCache();
		cacheutils::dropPrimProcFdCache();
		flushInodeCache();

		DeviceDBRootList::iterator pt = moduledbrootlist.begin();
		for( ; pt != moduledbrootlist.end() ; pt++)
		{
			int moduleID = (*pt).DeviceID;

			DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();
			for( ; pt1 != (*pt).dbrootConfigList.end() ;pt1++)
			{
				int id = *pt1;

				if (config.moduleID() != moduleID)
				{
					if ( GlusterConfig == "n" )
					{
						string cmd = "umount " + startup::StartUp::installDir() + "/data" + oam.itoa(id) + " > /dev/null 2>&1";
						system(cmd.c_str());
					}
					else
					{
						try {
							string moduleid = oam.itoa(config.moduleID());
							string errmsg;
							int ret = oam.glusterctl(oam::GLUSTER_UNASSIGN, oam.itoa(id), moduleid, errmsg);
							if ( ret != 0 )
							{
								log.writeLog(__LINE__, "Error unassigning gluster dbroot# " + oam.itoa(id) + ", error: " + errmsg, LOG_TYPE_ERROR);
							}
						}
						catch (...)
						{
							log.writeLog(__LINE__, "Exception unassigning gluster dbroot# " + oam.itoa(id), LOG_TYPE_ERROR);
						}
					}
				}
			}
		}
	}
	catch(...)
	{}

	return;
}

/******************************************************************************************
* @brief	checkDataMount
*
* purpose:	Check Data Mounts
*
*
******************************************************************************************/
int ProcessMonitor::checkDataMount()
{
	MonitorLog log;
	ModuleConfig moduleconfig;
	Oam oam;

	//check/update the pmMount files

	log.writeLog(__LINE__, "checkDataMount called ", LOG_TYPE_DEBUG);

	string DBRootStorageType = "internal";
	vector <string> dbrootList;
	string installDir(startup::StartUp::installDir());

	for ( int retry = 0 ; retry < 10 ; retry++)
	{
		try
		{
			systemStorageInfo_t t;
			t = oam.getStorageConfig();
	
			if ( boost::get<1>(t) == 0 ) {
				log.writeLog(__LINE__, "getStorageConfig return: No dbroots are configured in Calpont.xml file", LOG_TYPE_WARNING);
				return API_INVALID_PARAMETER;
			}
	
			DeviceDBRootList moduledbrootlist = boost::get<2>(t);
	
			DeviceDBRootList::iterator pt = moduledbrootlist.begin();
			for( ; pt != moduledbrootlist.end() ; pt++)
			{
				int moduleID = (*pt).DeviceID;
	
				DBRootConfigList::iterator pt1 = (*pt).dbrootConfigList.begin();
				for( ; pt1 != (*pt).dbrootConfigList.end() ;pt1++)
				{
					if (config.moduleID() == moduleID)
					{
						dbrootList.push_back(oam.itoa(*pt1));
					}
				}
			}

			break;
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on getStorageConfig: " + error, LOG_TYPE_ERROR);
			sleep (1);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on getStorageConfig: Caught unknown exception!", LOG_TYPE_ERROR);
			sleep (1);
		}
	}

	if ( dbrootList.size() == 0 ) {
		log.writeLog(__LINE__, "No dbroots are configured in Calpont.xml file", LOG_TYPE_WARNING);
		return API_INVALID_PARAMETER;
	}

	try{
		oam.getSystemConfig("DBRootStorageType", DBRootStorageType);
	}
	catch(...) {}

	//asign dbroot is gluster
	if (GlusterConfig == "y")
	{
		vector<string>::iterator p = dbrootList.begin();
		while ( p != dbrootList.end() )
		{
			string dbrootID = *p;
			p++;

			try {
				string moduleid = oam.itoa(config.moduleID());
				string errmsg;
				int ret = oam.glusterctl(oam::GLUSTER_ASSIGN, dbrootID, moduleid, errmsg);
				if ( ret != 0 )
				{
					log.writeLog(__LINE__, "Error assigning gluster dbroot# " + dbrootID + ", error: " + errmsg, LOG_TYPE_ERROR);
				}
			}
			catch (...)
			{
				log.writeLog(__LINE__, "Exception assigning gluster dbroot# " + dbrootID, LOG_TYPE_ERROR);
			}
		}

		//go unmount disk NOT assigned to this pm
		unmountExtraDBroots();
	}

	if ( DBRootStorageType == "hdfs" ||
		(DBRootStorageType == "internal" && GlusterConfig == "n") ) {
		//create OAM-Test-Flag
		vector<string>::iterator p = dbrootList.begin();
		while ( p != dbrootList.end() )
		{
			string dbroot = installDir + "/data" + *p;
			p++;
	
			string fileName = dbroot + "/OAMdbrootCheck";
			ofstream fout(fileName.c_str());
			if (!fout) {
				log.writeLog(__LINE__, "ERROR: Failed test write to dbroot: "  + dbroot, LOG_TYPE_ERROR);

				return API_FAILURE;
			}
		}

		return API_SUCCESS;
	}

	//Flush the cache
	cacheutils::flushPrimProcCache();
	cacheutils::dropPrimProcFdCache();
	flushInodeCache();

	//external or gluster
	vector<string>::iterator p = dbrootList.begin();
	while ( p != dbrootList.end() )
	{
		string dbroot = installDir + "/data" + *p;
		string fileName = dbroot + "/OAMdbrootCheck";

		if ( GlusterConfig == "n" ) {
			//remove any local check flag for starters
			string cmd = "umount " + dbroot + " > /tmp/mount.txt 2>&1";
			system(cmd.c_str());
	
			unlink(fileName.c_str());
	
			cmd = "mount " + dbroot + " > /tmp/mount.txt 2>&1";
			system(cmd.c_str());
	
			if ( !rootUser) {
				cmd = "sudo chown -R " + USER + ":" + USER + " " + dbroot + " > /dev/null";
				system(cmd.c_str());
			}

			ifstream in("/tmp/mount.txt");
	
			in.seekg(0, std::ios::end);
			int size = in.tellg();
			if ( size != 0 )
			{
				if (!oam.checkLogStatus("/tmp/mount.txt", "already")) {
					log.writeLog(__LINE__, "checkDataMount: mount failed, dbroot: " + dbroot, LOG_TYPE_ERROR);
	
					try{
						oam.setDbrootStatus(*p, oam::AUTO_OFFLINE);
					}
					catch (exception& ex)
					{}
		
					return API_FAILURE;
				}
			}

			log.writeLog(__LINE__, "checkDataMount: successfull mount " + dbroot, LOG_TYPE_DEBUG);
		}

		//create OAM-Test-Flag check rw mount
		ofstream fout(fileName.c_str());
		if (!fout) {
			log.writeLog(__LINE__, "ERROR: Failed test write to dbroot: "  + dbroot, LOG_TYPE_ERROR);

			try{
				oam.setDbrootStatus(*p, oam::AUTO_OFFLINE);
			}
			catch (exception& ex)
			{}

			return API_FAILURE;
		}

		try{
			oam.setDbrootStatus(*p, oam::ACTIVE);
		}
		catch (exception& ex)
		{}

		p++;
	}

	return API_SUCCESS;
}


/******************************************************************************************
* @brief	calTotalUmMemory
*
* purpose:	Calculate TotalUmMemory
*
*
******************************************************************************************/
void ProcessMonitor::calTotalUmMemory()
{
	MonitorLog log;
	Oam oam;

 	struct sysinfo myinfo; 

	//check/update the pmMount files

	log.writeLog(__LINE__, "calTotalUmMemory called ", LOG_TYPE_DEBUG);

	try{
		sysinfo(&myinfo);
	}
	catch (...) {
		return;
	}

	//get memory stats
	long long total = myinfo.totalram / 1024 / 1000;

	// adjust max memory, 25% of total memory
	string value;

	if ( total <= 2000 )
		value = "256M";
	else if ( total <= 4000 )
		value = "512M";
	else if ( total <= 8000 )
		value = "1G";
	else if ( total <= 16000 )
		value = "2G";
	else if ( total <= 32000 )
		value = "4G";
	else if ( total <= 64000 )
		value = "8G";
	else
		value = "16G";

	try {
		Config* sysConfig = Config::makeConfig();
		sysConfig->setConfig("HashJoin", "TotalUmMemory", value);

		//update Calpont Config table
		try {
			sysConfig->write();
		}
		catch(...)
		{
			log.writeLog(__LINE__, "ERROR: sysConfig->write", LOG_TYPE_ERROR);
			return;
		}

		log.writeLog(__LINE__, "set TotalUmMemory to " + value, LOG_TYPE_DEBUG);
	}
	catch (...) {
		log.writeLog(__LINE__, "Failed to set TotalUmMemory to " + value, LOG_TYPE_ERROR);
	}

	return;

}

/******************************************************************************************
* @brief	flushInodeCache
*
* purpose:	flush cache
*
*
******************************************************************************************/
void ProcessMonitor::flushInodeCache()
{
	int fd;
	ByteStream reply;

#ifdef __linux__
	fd = open("/proc/sys/vm/drop_caches", O_WRONLY);
	if (fd >= 0) {
		if (write(fd, "3\n", 2) == 2)
		{
			log.writeLog(__LINE__, "flushInodeCache successful", LOG_TYPE_DEBUG);
		}
		else {
			log.writeLog(__LINE__, "flushInodeCache failed", LOG_TYPE_DEBUG);
		}
		close(fd);
	}
	else {
		log.writeLog(__LINE__, "flushInodeCache failed to open file", LOG_TYPE_DEBUG);
	}
#endif
}


} //end of namespace
// vim:ts=4 sw=4:

