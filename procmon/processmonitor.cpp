/***************************************************************************
* $Id: processmonitor.cpp 1978 2013-02-08 20:10:28Z dhill $
*
 ***************************************************************************/

#include "processmonitor.h"

using namespace std;
using namespace oam;
using namespace messageqcpp;
using namespace snmpmanager;
using namespace logging;
using namespace config;

extern string systemOAM;
extern string dm_server;
extern bool runStandby;
extern bool gsharedNothingFlag;
extern bool processInitComplete;
extern int fmoduleNumber;

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
						if ( actIndicator != oam::GRACEFUL_STANDBY) {
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
								break;
							}
						}
						else
						{
							//this module going Parent OAM Standby
							runStandby = true;
							log.writeLog(__LINE__,  "ProcMon Running going Hot-Standby");

							// delete any old active alarm log file
							unlink ("/var/log/Calpont/activeAlarms");
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

					//Inform Process Manager that Process restart
					processRestarted(processName);

					ackMsg << (ByteStream::byte) ACK;
					ackMsg << (ByteStream::byte) START;
					ackMsg << (ByteStream::byte) requestStatus;
					mq.write(ackMsg);
	
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
					int requestStatus = API_SUCCESS;
					log.writeLog(__LINE__, "MSG RECEIVED: Re-Init process request on: " + processName);
		
					processList::iterator listPtr;
					processList* aPtr = config.monitoredListPtr();
					listPtr = aPtr->begin();
		
					for (; listPtr != aPtr->end(); ++listPtr)
					{
						if ((*listPtr).ProcessName == processName) {
							if ( (*listPtr).processID <= 1 ) {
								log.writeLog(__LINE__,  "ERROR: process not active" , LOG_TYPE_DEBUG );
								requestStatus = API_SUCCESS;
								break;
							}

							reinitProcess((*listPtr).processID, (*listPtr).ProcessName, actIndicator);
							requestStatus = API_SUCCESS;
							break;
						}
					}

					if (listPtr == aPtr->end())
					{
						log.writeLog(__LINE__,  "ERROR: No such process: " + processName, LOG_TYPE_ERROR );
						requestStatus = API_FAILURE;
					}

					ackMsg << (ByteStream::byte) ACK;
					ackMsg << (ByteStream::byte) PROCREINITPROCESS;
					ackMsg << (ByteStream::byte) requestStatus;
					mq.write(ackMsg);

					log.writeLog(__LINE__, "PROCREINITPROCESS: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));
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
						system("/usr/local/Calpont/bin/reset_locks  > /var/log/Calpont/reset_locks.log1 2>&1");
						log.writeLog(__LINE__, "BRM reset_locks script run", LOG_TYPE_DEBUG);

						//stop the mysql daemon
						try {
							oam.actionMysqlCalpont(MYSQL_STOP);
						}
						catch(...)
						{}

						//send down notification
						oam.sendDeviceNotification(config.moduleName(), MODULE_DOWN);
					}

					ackMsg << (ByteStream::byte) ACK;
					ackMsg << (ByteStream::byte) STOPALL;
					ackMsg << (ByteStream::byte) requestStatus;
					mq.write(ackMsg);

					log.writeLog(__LINE__, "STOPALL: ACK back to ProcMgr, return status = " + oam.itoa((int) requestStatus));

					//Remove Calpont RPM on REMOVE option
					if ( actIndicator == oam::REMOVE ) {
						if ( config.moduleType() != "xm" ) {
							log.writeLog(__LINE__,  "STOPALL: uninstall Calpont RPMs", LOG_TYPE_DEBUG);
							if ( config.moduleType() == "um" ) {
								system("rpm -e calpont calpont-mysqld calpont-mysql --nodeps");
								system("dpkg -P calpont calpont-mysqld calpont-mysql");
							}
							else	// pm
							{
								system("umount /usr/local/Calpont/data* -l > /dev/null 2>&1");
								sleep(1);
		
								system("rpm -e calpont calpont-mysqld calpont-mysql --nodeps");
								system("dpkg -P calpont calpont-mysqld calpont-mysql");
							}
							// should get here is packages get removed correctly
							system("/etc/init.d/infinidb stop > /dev/null 2>&1");
							exit (0);
						}
						else
						{
							log.writeLog(__LINE__, "STOPALL: REMOVE OPTION - removing XM ProcMon setup", LOG_TYPE_DEBUG);

							requestStatus = removeXMProcMon();
						}
					}

					break;
				}
				case STARTALL:
				{
					msg >> manualFlag;
					int requestStatus = oam::API_SUCCESS;
					log.writeLog(__LINE__,  "MSG RECEIVED: Start All process request...");

					//if non parent PM module, remount as read-only to flush the cache
					if ( config.moduleType() == "pm" && !gOAMParentModuleFlag && !gsharedNothingFlag) {
						int ret = setDataMount("ro");
						if ( ret == oam::API_FAILURE ) {
							//send notification about the mount setup failure
							oam.sendDeviceNotification(config.moduleName(), DBROOT_MOUNT_FAILURE);

							ackMsg << (ByteStream::byte) ACK;
							ackMsg << (ByteStream::byte) STARTALL;
							ackMsg << (ByteStream::byte) oam::API_FAILURE;
							mq.write(ackMsg);
							break;
						}
					}
		
					// parent PM Module, remount read-write
					if ( gOAMParentModuleFlag ) {
						int ret = setDataMount("rw");
						if ( ret == oam::API_FAILURE ) {
							//send notification about the mount setup failure
							oam.sendDeviceNotification(config.moduleName(), DBROOT_MOUNT_FAILURE);

							ackMsg << (ByteStream::byte) ACK;
							ackMsg << (ByteStream::byte) STARTALL;
							ackMsg << (ByteStream::byte) oam::API_FAILURE;
							mq.write(ackMsg);
							break;
						}
					}
		
					//Loop through all Process belong to this module	
					processList::iterator listPtr;
					processList* aPtr = config.monitoredListPtr();
					listPtr = aPtr->begin();
					requestStatus = API_SUCCESS;
					bool procmonProcessStart = false;
	
					//launched any processes controlled by ProcMon that aren't active
					for (; listPtr != aPtr->end(); ++listPtr)
					{
						if ( (*listPtr).BootLaunch != BOOT_LAUNCH)
							continue;
		
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
								procmonProcessStart = true;
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
								// error in launching a process
									break;
								else
								{
									//run startup test script to perform basic DB sanity testing
									if ( config.moduleType() != "xm" ) {
										if ( config.moduleName() == config.OAMParentName() 
												&& (*listPtr).ProcessName == "DBRMWorkerNode" ) {
											if ( runStartupTest() != oam::API_SUCCESS ){
												requestStatus = oam::API_FAILURE_DB_ERROR;
												break;
											}
										}
									}
								}
//								sleep(2);
							}
							else
							{	// if DBRMWorkerNode and ACTIVE, run runStartupTest
								if ( config.moduleName() == config.OAMParentName() 
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

						//start the mysql daemon 
						try {
							oam.actionMysqlCalpont(MYSQL_START);
						}
						catch(...)
						{}

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
			
					processList* aPtr = config.monitoredListPtr();
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

					ackMsg << (ByteStream::byte) ACK;
					ackMsg << (ByteStream::byte) SHUTDOWNMODULE;
					ackMsg << (ByteStream::byte) API_SUCCESS;
					mq.write(ackMsg);

					log.writeLog(__LINE__, "SHUTDOWNMODULE: ACK back to ProcMgr, return status = " + oam.itoa((int) API_SUCCESS));

					//sleep to give time for process-manager to finish up
					sleep(5);
					system("/etc/init.d/infinidb stop > /dev/null 2>&1");
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
				system("rpm -e calpont-mysql calpont-mysqld");
				system("dpkg -P calpont-mysql calpont-mysqld");
				log.writeLog(__LINE__, "RECONFIGURE: removed mysql rpms");
			}

			// install mysql rpms if being reconfigured as a um
			if ( reconfigureModuleName.find("um") != string::npos ) {
				system("rpm -ivh /root/calpont-mysql-*rpm /root/calpont-mysqld-*rpm > /tmp/rpminstall");
				system("dpgk -i /root/calpont-mysql-*deb /root/calpont-mysqld-*deb >> /tmp/rpminstall");

				system("/usr/local/Calpont/bin/post-mysqld-install >> /tmp/rpminstall");
//				sleep(5);
				system("/usr/local/Calpont/bin/post-mysql-install >> /tmp/rpminstall");
//				sleep(5);
				system("/etc/init.d/mysql-Calpont start > /tmp/mysqldstart");
//				sleep(5);

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

		case UPDATEEXPORTS:
		{
			log.writeLog(__LINE__,  "MSG RECEIVED: Update exports file");

			string IPAddress;

			msg >> IPAddress;

			uint16_t rtnCode = API_SUCCESS;
			int requestStatus = API_SUCCESS;

			if (rtnCode)
				// error in updating log
				requestStatus = API_FAILURE;

			ackMsg << (ByteStream::byte) ACK;
			ackMsg << (ByteStream::byte) UPDATEEXPORTS;
			ackMsg << (ByteStream::byte) requestStatus;
			mq.write(ackMsg);

			log.writeLog(__LINE__, "UPDATEEXPORTS: ACK back to ProcMgr");

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

			uint16_t rtnCode;
			int requestStatus = API_SUCCESS;

			rtnCode = updateConfigFile(msg);
			if (rtnCode)
				// error in updating log
				requestStatus = API_FAILURE;

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

	if ( config.moduleType() != "xm" ) {
		//now do a pkill on process just to make sure all is clean
		string::size_type pos = processLocation.find("bin/",0);
		string procName = processLocation.substr(pos+4, 80);
		string cmd = "pkill -9 " + procName;
		system(cmd.c_str());
		log.writeLog(__LINE__, "Pkill Process just to make sure: " + procName, LOG_TYPE_DEBUG);
	}

	if (processName == "PrimProc")
		system("pkill DecomSvr >/dev/null 2>&1");

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
	ifstream file (processLocation.c_str());
	if (!file) {
		log.writeLog(__LINE__, "Process location: " + processLocation + " not found", LOG_TYPE_ERROR);
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
							opState == oam::AUTO_OFFLINE )
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
			processName == "DBRMControllerNode" ||
			processName == "DDLProc" ||
			processName == "DMLProc" ) {
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
			}
			catch(...)
			{
				log.writeLog(__LINE__, "EXCEPTION ERROR on getLocalDBRMID: no DBRM for module", LOG_TYPE_ERROR);
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
	if ( actIndicator != oam::GRACEFUL ) {
		if ( processName == "DBRMWorkerNode" ) {
			string DBRMDir;
			string tempDBRMDir = "/usr/local/Calpont/data/dbrm";
	
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
				return oam::API_FAILURE;
			}
	
			//create dbrm directory, just to make sure its there
			string cmd = "mkdir -p " + DBRMDir + " > /dev/null 2>&1";
			system(cmd.c_str());
	
			// if Non Parent OAM Module, get the dbmr data from Parent OAM Module
			if ( !gOAMParentModuleFlag ) {
	
				//create temp dbrm directory
				string cmd = "mkdir " + tempDBRMDir + " > /dev/null 2>&1";
				system(cmd.c_str());
	
				//setup softlink for editem on the 'um' or shared-nothing non active pm
				if( config.moduleType() == "um" || 
					(config.moduleType() == "pm" && gsharedNothingFlag) ) {
					cmd = "mv -f " + DBRMDir + " /root/ > /dev/null 2>&1";
					system(cmd.c_str());
		
					cmd = "ln -s " + tempDBRMDir + " " + DBRMDir + " > /dev/null 2>&1";
					system(cmd.c_str());
				}
	
				//change DBRMDir to temp DBRMDir
				DBRMDir = tempDBRMDir;

				// remove all files for temp directory
				cmd = "rm -f " + DBRMDir + "/*";
				system(cmd.c_str());

				// go request files for parent OAM module
				if ( getDBRMdata() != oam::API_SUCCESS ) {
					log.writeLog(__LINE__, "Error: getDBRMdata failed", LOG_TYPE_ERROR);
					sendAlarm("DBRM", DBRM_LOAD_DATA_ERROR, SET);	
					return oam::API_FAILURE;
				}
				// change DBRMroot tot temp DBRMDir path
				DBRMroot = tempDBRMDir + "/BRM_saves";
			}
	
			//
			// run the 'load_brm' script first if files exist
			//
			string loadScript = "load_brm";
	
			string fileName = DBRMroot + "_current";
	
			ifstream oldFile (fileName.c_str());
			if (oldFile) {
				char line[200];
				oldFile.getline(line, 200);
				string dbrmFile = line;
	
				if ( !gOAMParentModuleFlag ) {
	
					string::size_type pos = dbrmFile.find("/BRM_saves",0);
					if (pos != string::npos)
						dbrmFile = tempDBRMDir + dbrmFile.substr(pos,80);;
				}
	
				system("/usr/local/Calpont/bin/reset_locks  > /var/log/Calpont/reset_locks.log1 2>&1");
				log.writeLog(__LINE__, "BRM reset_locks script run", LOG_TYPE_DEBUG);
	
				system("/usr/local/Calpont/bin/clearShm -c  > /dev/null 2>&1");
				log.writeLog(__LINE__, "Clear Shared Memory script run", LOG_TYPE_DEBUG);
	
				string cmd = "/usr/local/Calpont/bin/" + loadScript + " " + dbrmFile + " > /var/log/Calpont/load_brm.log1 2>&1";
				log.writeLog(__LINE__, loadScript + " cmd = " + cmd, LOG_TYPE_DEBUG);
				system(cmd.c_str());
				
				if (oam.checkLogStatus("/var/log/Calpont/load_brm.log1", "OK"))
					log.writeLog(__LINE__, "Successfully return from " + loadScript, LOG_TYPE_DEBUG);
				else {
					log.writeLog(__LINE__, "Error return DBRM " + loadScript, LOG_TYPE_ERROR);
					sendAlarm("DBRM", DBRM_LOAD_DATA_ERROR, SET);
					return oam::API_FAILURE;
				}
	
				// now delete the dbrm data from local disk
				if ( !gOAMParentModuleFlag) {
					string cmd = "rm -f " + tempDBRMDir + "/*";
					system(cmd.c_str());
					log.writeLog(__LINE__, "removed DBRM file with command: " + cmd, LOG_TYPE_DEBUG);
				}
			}
			else
				log.writeLog(__LINE__, "No DBRM files exist, must be a initial startup", LOG_TYPE_DEBUG);
		}
	
		sendAlarm("DBRM", DBRM_LOAD_DATA_ERROR, CLEAR);
	}

	if ( config.moduleType() != "xm" ) {
		//do a pkill on process just to make sure there is no rouge version running
		string::size_type pos = processLocation.find("bin/",0);
		string procName = processLocation.substr(pos+4, 80);
		string cmd = "pkill -9 " + procName;
		system(cmd.c_str());
		log.writeLog(__LINE__, "Pkill Process just to make sure: " + procName, LOG_TYPE_DEBUG);
	}

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

	string outFileName = "/var/log/Calpont/" + processName + ".out";
	string errFileName = "/var/log/Calpont/" + processName + ".err";

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

		if (processLocation.find("snmp") != string::npos)
		{
			// snmpd app is special...........
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
	
			//Update ProcessConfig.xml: Mark Process oam::ACTIVE state 
			updateProcessInfo(processName, oam::ACTIVE, newProcessID);

		}
		else
		{
			//FYI - NEEDS TO STAY HERE TO HAVE newProcessID
	
			//record the process information into processList 
			config.buildList(processModuleType, processName, processLocation, arg_list, 
								launchID, newProcessID, initType, BootLaunch, RunType,
								DepProcessName, DepModuleName, LogFile);
	
			//Update ProcessConfig.xml: Mark Process INIT state 
			updateProcessInfo(processName, initType, newProcessID);
		}

		log.writeLog(__LINE__, processName + " PID is " + oam.itoa(newProcessID), LOG_TYPE_DEBUG);

		sendAlarm(processName, PROCESS_DOWN_MANUAL, CLEAR);
		sendAlarm(processName, PROCESS_DOWN_AUTO, CLEAR);
		sendAlarm(processName, PROCESS_INIT_FAILURE, CLEAR);

		//give time to get status updates from process before starting next process
		//if not, ExeMgr will fail since the previous started PrimProc is not ACTIVE
		sleep(1);

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

 		// open STDIN, STDOUT & STDERR for trapDaemon
		if (processName == "SNMPTrapDaemon")
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

	if ( config.moduleType() == "xm" )
		return;

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

	log.writeLog(__LINE__, "StatusUpdate of Process " + processName + " State = " + oam.itoa(state), LOG_TYPE_DEBUG);

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
		oam.setProcessStatus(processName, config.moduleName(), state, (int) PID);
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

	//restart syslog and update file priviledges
	changeModLog();

	return 0;
}

/******************************************************************************************
* @brief	syslogAction
*
* purpose:	Take Action on Syslog Process
*
******************************************************************************************/
/*void ProcessMonitor::syslogAction( std::string action)
{
	MonitorLog log;
	Oam oam;

	string systemlog = "syslog";

	string fileName;
	oam.getSystemConfig("SystemLogConfigFile", fileName);
	if (fileName == oam::UnassignedName ) {
		// unassigned
		log.writeLog(__LINE__, "ERROR: syslog file not configured ", LOG_TYPE_ERROR );
		return;
	}

	string::size_type pos = fileName.find("syslog-ng",0);
	if (pos != string::npos)
		systemlog = "syslog-ng";
	else
	{
		pos = fileName.find("rsyslog",0);
		if (pos != string::npos)
			systemlog = "rsyslog";	
	}

	string cmd;
	if ( action == "sighup" ) {
		if ( systemlog == "syslog" || systemlog == "rsyslog")
			systemlog = systemlog + "d";
		cmd = "pkill -hup " + systemlog + " > /dev/null 2>&1";
	}
	else
		cmd = "/etc/init.d/" + systemlog + " " + action + " > /dev/null 2>&1";

	// take action on syslog service to make sure it running
	system(cmd.c_str());

	// if start/restart, delay to give time for syslog to get up and going
	pos = action.find("start",0);
	if (pos != string::npos)
		sleep(2);
}
*/
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
			|| systemprocessconfig.processconfig[i].ModuleType == "ChildExtOAMModule"
			|| ( systemprocessconfig.processconfig[i].ModuleType == "ChildOAMModule" && config.moduleType() != "xm" )
			|| (systemprocessconfig.processconfig[i].ModuleType == "ParentOAMModule" &&
				systemModuleType == OAMParentModuleType) )
		{
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

	ifstream oldFile (fileName.c_str());
	if (!oldFile) {
		system("/usr/local/Calpont/bin/dbbuilder 7 > /var/log/Calpont/dbbuilder.log &");

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

	//create custom fstab into /usr/local/Calpont/local/etc/"modulename"
	string dir = "/usr/local/Calpont/local/etc/" + reconfigureModuleName;

	string cmd = "mkdir " + dir + " > /dev/null 2>&1";
	system(cmd.c_str());

	cmd = "rm -f " + dir + "/* > /dev/null 2>&1";
	system(cmd.c_str());

	if ( reconfigureModuleName.find("um") != string::npos) {

		//check if /um1/fstab exist, error out if it doesn't
		ifstream file ("/usr/local/Calpont/local/etc/um1/fstab.calpont");
		if (!file) {
			log.writeLog(__LINE__, "reconfigureModule - FAILED: missing um1 fstab file", LOG_TYPE_ERROR);
			return API_FAILURE;
		}

		cmd = "cp /usr/local/Calpont/local/etc/um1/* " + dir + "/.";
		system(cmd.c_str());
	}
	else
	{
		ifstream file ("/usr/local/Calpont/local/etc/pm1/fstab.calpont");
		if (!file) {
			log.writeLog(__LINE__, "reconfigureModule - FAILED: missing pm1 fstab file", LOG_TYPE_ERROR);
			return API_FAILURE;
		}

		cmd = "cp /usr/local/Calpont/local/etc/pm1/* " + dir + "/.";
		system(cmd.c_str());
	}

	log.writeLog(__LINE__, "reconfigureModule - custom fstab created for " +  reconfigureModuleName, LOG_TYPE_DEBUG);

	//apply new fstab.calpont
	cmd = "rm -f /tmp/fstab.calpont";
	system(cmd.c_str());
	cmd = "cp /usr/local/Calpont/local/etc/" + reconfigureModuleName + "/fstab.calpont /usr/local/Calpont/local/.";
	system(cmd.c_str());
	cmd = "rm -f /etc/fstab";
	system(cmd.c_str());
	cmd = "cp /etc/fstab.calpontSave /etc/fstab";
	system(cmd.c_str());
	cmd = "cat /usr/local/Calpont/local/fstab.calpont >> /etc/fstab";
	system(cmd.c_str());
	cmd = "mount -al > /dev/null 2>&1";
	system(cmd.c_str());

	//copy and apply new rc.local.calpont from dm1
	cmd = "rm -f /usr/local/Calpont/local/rc.local.calpont";
	system(cmd.c_str());
	cmd = "cp /usr/local/Calpont/local/etc/" + reconfigureModuleName + "/rc.local.calpont /usr/local/Calpont/local/.";
	system(cmd.c_str());
	cmd = "rm -f /etc/rc.d/rc.local";
	system(cmd.c_str());
	cmd = "cp /etc/rc.d/rc.local.calpontSave /etc/rc.d/rc.local";
	system(cmd.c_str());
	cmd = "cat /usr/local/Calpont/local/rc.local.calpont >> /etc/rc.d/rc.local";
	system(cmd.c_str());
	cmd = "/etc/rc.d/rc.local";
	system(cmd.c_str());

	//update module file
	string fileName = "/usr/local/Calpont/local/module";

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

		if ( runType == SIMPLEX && gOAMParentModuleFlag )
			retStatus = oam::MAN_INIT;
		else
			if ( runType == SIMPLEX && !gOAMParentModuleFlag )
				retStatus = oam::COLD_STANDBY;
				else
					if ( runType == ACTIVE_STANDBY && processModuleType == "ParentOAMModule" && gOAMParentModuleFlag )
						retStatus = oam::MAN_INIT;
					else
						if ( runType == ACTIVE_STANDBY && processModuleType == "ParentOAMModule" && config.OAMStandbyParentFlag() )
							retStatus = oam::STANDBY;
						else
							if ( runType == ACTIVE_STANDBY && processModuleType == "ParentOAMModule" )
								retStatus = oam::COLD_STANDBY;
							else
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

	if ( retStatus == oam::COLD_STANDBY || retStatus == oam::STANDBY ) {
		updateProcessInfo(processName, retStatus, 0);
	
		sendAlarm(processName, PROCESS_DOWN_MANUAL, CLEAR);
		sendAlarm(processName, PROCESS_DOWN_AUTO, CLEAR);
		sendAlarm(processName, PROCESS_INIT_FAILURE, CLEAR);
	}

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
int ProcessMonitor::createDataDirs(std::string option)
{
	MonitorLog log;
	Oam oam;

	log.writeLog(__LINE__, "createDataDirs called with option " + option, LOG_TYPE_DEBUG);

	if ( option == "storage" ) {
		// create data directories on mounted disk
		if ( config.moduleType() == "pm" || config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM ) {
			string fileName = "/etc/fstab";
		
			ifstream file (fileName.c_str());
		
			vector <string> lines;
			char line[200];
			string buf;
		
			while (file.getline(line, 200))
			{
				buf = line;
		
				string::size_type pos = buf.find("/usr/local/Calpont/data",0);
				if (pos != string::npos) {
					//found one, read path and create
					string::size_type pos1 = buf.find(" ",pos);
					string directory = buf.substr(pos,pos1-pos);
					string cmd = "mkdir " + directory + " > /dev/null 2>&1";
					int rtnCode = system(cmd.c_str());
					if (rtnCode == 0)
						log.writeLog(__LINE__, "Successful created directory " + directory, LOG_TYPE_DEBUG);
				}
			}
			file.close();
		}
	}
	else	// local
	{
		int DBRootCount;
		try{
			oam.getSystemConfig("DBRootCount", DBRootCount);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig, DBRootCount", LOG_TYPE_ERROR);
			return API_FAILURE;
		}

		for ( int i = 1 ; i < DBRootCount+1 ; i++)
		{
			string DBRootName;
			string DBroot = "DBRoot" + oam.itoa(i);

			try{
				oam.getSystemConfig(DBroot, DBRootName);
			}
			catch(...)
			{
				log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig, " + DBRootName, LOG_TYPE_ERROR);
				return API_FAILURE;
			}

			string cmd = "mkdir " + DBRootName + " > /dev/null 2>&1";;
			int rtnCode = system(cmd.c_str());
			if (rtnCode == 0)
				log.writeLog(__LINE__, "Successful created directory " + DBRootName, LOG_TYPE_DEBUG);

			cmd = "chmod 1777 " + DBRootName + " > /dev/null 2>&1";
			system(cmd.c_str());
		}
	}

	return API_SUCCESS;
}

/******************************************************************************************
* @brief	createPMDirs
*
* purpose:	Create the PM mount directories
*
*
******************************************************************************************/
int ProcessMonitor::setupPMmount()
{
/*	Oam oam;

	// setup actPM path
	string parentOAMModule;
	oam.getSystemConfig("ParentOAMModuleName", parentOAMModule);

	if ( parentOAMModule == oam::UnassignedName )
		// default pm1
		parentOAMModule = "pm1";

	system("rm -f /mnt/actPM_systemFiles");
	string cmd = "ln -s /mnt/" + parentOAMModule + "_systemFiles /mnt/actPM_systemFiles > /dev/null 2>&1";
	system(cmd.c_str());

	system("rm -fr /usr/local/Calpont/data1/systemFiles/dbrm");
	cmd = "ln -s /mnt/actPM_systemFiles/dbrm /usr/local/Calpont/data1/systemFiles/dbrm";
	system(cmd.c_str());

	system("rm -fr /usr/local/Calpont/etc");
	cmd = "ln -s /mnt/actPM_systemFiles/etc/ /usr/local/Calpont/etc";
	system(cmd.c_str());
*/
	return API_SUCCESS;
}

/******************************************************************************************
* @brief	setDataMount
*
* purpose:	Set Data Mounts, option = rw for read/write , ro for read-only
*
*
******************************************************************************************/
int ProcessMonitor::setDataMount( std::string option )
{
	MonitorLog log;
//	MonitorConfig config;
//	ProcessMonitor aMonitor(config, log);
	ModuleConfig moduleconfig;
	Oam oam;

	//check/update the pmMount files

	log.writeLog(__LINE__, "setDataMount called with option "  + option, LOG_TYPE_DEBUG);

	string DBRootStorageType = "local";
	vector <string> dbrootList;

	// get dbroot list and storage type from config file
	for ( int id = 1 ;; id++ )
	{
		string dbroot = "DBRoot" + oam.itoa(id);

		string dbootDir;
		try{
			oam.getSystemConfig(dbroot, dbootDir);
		}
		catch(...) {}

		if ( dbootDir.empty() || dbootDir == "" )
			break;

		dbrootList.push_back(dbootDir);
	}

	if ( dbrootList.size() == 0 ) {
		log.writeLog(__LINE__, "ERROR: No dbroots are configured in Calpont.xml file", LOG_TYPE_CRITICAL);
		//Set the alarm 
		sendAlarm(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, SET);
		return API_FAILURE;
	}

	try{
		oam.getSystemConfig("DBRootStorageType", DBRootStorageType);
	}
	catch(...) {}
 
	if ( DBRootStorageType != "storage" ) {
		if ( option == "rw" ) {
			//create OAM-Test-Flag
			vector<string>::iterator p = dbrootList.begin();
			while ( p != dbrootList.end() )
			{
				string dbroot = *p;
				p++;
		
				string fileName = dbroot + "/OAMdbrootCheck";
				ofstream fout(fileName.c_str());
				if (!fout) {
					log.writeLog(__LINE__, "ERROR: Failed test write to dbroot: "  + dbroot, LOG_TYPE_ERROR);
	
					return API_FAILURE;
				}
			}

			//setup TransactionLog on mounted Performance Module
			setTransactionLog(true);
		}
		else
			//clear TransactionLog on mounted Performance Module
			setTransactionLog(false);

		return API_SUCCESS;
	}

	// update mount type
	system("/usr/local/Calpont/bin/syslogSetup.sh uninstall  > /dev/null 2>&1");
	oam.syslogAction("sighup");

	string tmpFileName = "/etc/fstab";

	vector<string>::iterator p = dbrootList.begin();
	while ( p != dbrootList.end() )
	{
		string dbroot = *p;
		p++;

		string mountcmd = "umount " + dbroot + " > /dev/null 2>&1";
		system(mountcmd.c_str());

		mountcmd = "mount -o " + option + " " + dbroot + " > /tmp/mount 2>&1";
		system(mountcmd.c_str());

		if (oam.checkLogStatus("/tmp/mount", "not exist") ||
			oam.checkLogStatus("/tmp/mount", "can't find")) {
			system("/usr/local/Calpont/bin/syslogSetup.sh install  > /dev/null 2>&1");
			oam.syslogAction("sighup");
			log.writeLog(__LINE__, "ERROR: dbroot mount error " + dbroot, LOG_TYPE_WARNING);
			return oam::API_FAILURE;
		}

		//log.writeLog(__LINE__, "mount cmd performed = "  + mountcmd, LOG_TYPE_DEBUG);
		
		//create OAM-Test-Flag check rw mount
		if ( option == "rw" ) {
			string fileName = dbroot + "/OAMdbrootCheck";
			ofstream fout(fileName.c_str());
			if (!fout) {
				system("/usr/local/Calpont/bin/syslogSetup.sh install  > /dev/null 2>&1");
				oam.syslogAction("sighup");

				log.writeLog(__LINE__, "ERROR: Failed test write to dbroot: "  + dbroot, LOG_TYPE_ERROR);

				return API_FAILURE;
			}
		}
	}

	system("/usr/local/Calpont/bin/syslogSetup.sh install  > /dev/null 2>&1");
	oam.syslogAction("sighup");

	//TransactionLog
	if ( option == "rw" ) {
		//setup TransactionLog on mounted Performance Module
		setTransactionLog(true);
	}
	else
		//clear TransactionLog on mounted Performance Module
		setTransactionLog(false);

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
							string::size_type pos1 = fileName.find("/dbrm",0);
							pos = fileName.find("data1",0);
							if (pos != string::npos)
							{
								string temp = fileName.substr(0,pos);
								string temp1 = temp + "data" + fileName.substr(pos1,80);
								fileName = temp1;
							}

							ofstream out(fileName.c_str());

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
								out << receivedMSG;
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
						system("touch /usr/local/Calpont/data/dbrm/BRM_saves_journal");

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

	// restart the syslog service to make sure it running
//	log.writeLog(__LINE__, "Start and reload crond", LOG_TYPE_DEBUG);
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

	// restart the crond service to make sure it running
//	log.writeLog(__LINE__, "Start and reload crond", LOG_TYPE_DEBUG);
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

		system("cp /usr/local/Calpont/bin/transactionLog /etc/cron.d/.");
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
//	log.writeLog(__LINE__, "Start and reload crond", LOG_TYPE_DEBUG);
	system("/etc/init.d/crond start > /dev/null 2>&1");
	system("/etc/init.d/crond reload > /dev/null 2>&1");
}


/******************************************************************************************
* @brief	removeXMProcMon
*
* purpose:	Remove XM ProcMon setup
*
*
******************************************************************************************/
int ProcessMonitor::removeXMProcMon()
{

	//umount from System
	if ( umountSystem() != oam::API_SUCCESS) {
		log.writeLog(__LINE__, "removeXMProcMon - ERROR: failed to unmount form system", LOG_TYPE_ERROR );
		return API_FAILURE;
	}

	//remove associated /mnt/ files and directories
	string cmd = "rm -fr /mnt/" + dm_server + "*";
	int rtnCode = system(cmd.c_str());
	if (rtnCode == 0) {
		log.writeLog(__LINE__, "removeXMProcMon - Successfully removed directories", LOG_TYPE_DEBUG);
	}

	return API_SUCCESS;
}

/******************************************************************************************
* @brief	umountSystem
*
* purpose:	unmount from associated system
*
*
******************************************************************************************/
int ProcessMonitor::umountSystem()
{
	string fileName = "/mnt/" + dm_server + "_mount";

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return false;
	
	char line[200];
	string buf;

	while (oldFile.getline(line, 200))
	{
		buf = line;

		string::size_type pos = buf.find("umount",0);
		if (pos != string::npos)
			// run found unmount command
			system(buf.c_str());
	}

	oldFile.close();

	return API_SUCCESS;
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
	if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
		return oam::API_SUCCESS;

	//run startup test script
	system("/usr/local/Calpont/bin/startupTests.sh > /var/log/Calpont/startupTests.log1 2>&1");

	if (oam.checkLogStatus("/var/log/Calpont/startupTests.log1", "OK")) {
		log.writeLog(__LINE__, "Successfully ran startupTests", LOG_TYPE_DEBUG);
		//Clear the alarm 
		sendAlarm(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, CLEAR);
		return oam::API_SUCCESS;
	}
	else {
		log.writeLog(__LINE__, "ERROR: Failure returned from startupTests", LOG_TYPE_CRITICAL);
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
	string fileName;
	string calpontFile;

	const string defaultCalpontConfigFile("/usr/local/Calpont/etc/Calpont.xml");
	const string defaultCalpontConfigFileTemp("/usr/local/Calpont/etc/Calpont.xml.temp");
	const string tmpCalpontConfigFileTemp("/usr/local/Calpont/etc/Calpont.xml.temp1");
	const string saveCalpontConfigFileTemp("/usr/local/Calpont/etc/Calpont.xml.calpontSave");

	msg >> fileName;
	msg >> calpontFile;

	if ( fileName == defaultCalpontConfigFile ) {
		unlink (defaultCalpontConfigFileTemp.c_str());
	
		ofstream newFile (defaultCalpontConfigFileTemp.c_str());
	
		newFile.write (calpontFile.c_str(), calpontFile.size() );
	
		newFile.close();
	
		struct flock fl;
		int fd;
	
		fl.l_type   = F_WRLCK;  // write lock
		fl.l_whence = SEEK_SET;
		fl.l_start  = 0;
		fl.l_len    = 0;
		fl.l_pid    = getpid();
	
		fd = open(defaultCalpontConfigFile.c_str(), O_WRONLY);
	
		if (fcntl(fd, F_SETLKW, &fl) == -1) {
			log.writeLog(__LINE__, "ERROR: Config::write: file lock error", LOG_TYPE_ERROR);
			return oam::API_FAILURE;
		}
	
		//save copy, copy temp file tp tmp then to Calpont.xml
		//move to /tmp to get around a 'same file error' in mv command
		string cmd = "rm -f " + saveCalpontConfigFileTemp;
		system(cmd.c_str());
		cmd = "cp " + defaultCalpontConfigFile + " " + saveCalpontConfigFileTemp;
		system(cmd.c_str());
	
		cmd = "rm -f " + tmpCalpontConfigFileTemp;
		system(cmd.c_str());
		cmd = "mv -f " + defaultCalpontConfigFileTemp + " " + tmpCalpontConfigFileTemp;
		system(cmd.c_str());
	
		cmd = "mv -f " + tmpCalpontConfigFileTemp + " " + defaultCalpontConfigFile;
		system(cmd.c_str());
	
		fl.l_type   = F_UNLCK;	//unlock
		if (fcntl(fd, F_SETLK, &fl) == -1)
			throw runtime_error("Config::write: file unlock error " + defaultCalpontConfigFile);
	
		close(fd);
	}
	else
	{
		//update a non Calpont.xml file
		unlink (fileName.c_str());
	
		ofstream newFile (fileName.c_str());
	
		newFile.write (calpontFile.c_str(), calpontFile.size() );
	
		newFile.close();
	}

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
* purpose:	check if module failover is needed due to a process outage
*
******************************************************************************************/
void ProcessMonitor::checkProcessFailover( std::string processName)
{
	// only failover for processes running on the Active Parent OAM Module
	if ( !gOAMParentModuleFlag || 
		( config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM && (fmoduleNumber-1) == 1) )
		return;

	//check if license has expired
	oam::LicenseKeyChecker keyChecker;

	bool keyGood = true;
	try {
		keyGood = keyChecker.isGood();
	}
	catch(...)
	{
		keyGood = false;
	}

	if (!keyGood)
	{
		return;
	}

	//force failover on certain processes
	if ( processName == "DBRMControllerNode" ||
		processName == "DDLProc" ||
		processName == "DMLProc" ||
		processName == "ProcessManager" ) {
			log.writeLog(__LINE__, "checkProcessFailover: module failover, process outage of " + processName, LOG_TYPE_CRITICAL);

//			system("reboot");
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
		string cmd = "/usr/local/Calpont/bin/upgrade-infinidb.sh doupgrade --password=" + mysqlpw + "  > /var/log/Calpont/upgrade-infinidb.log1 2>&1";
		system(cmd.c_str());
	
		if (oam.checkLogStatus("/var/log/Calpont/upgrade-infinidb.log1", "OK")) {
			log.writeLog(__LINE__, "upgrade-infinidb.sh: Successful return", LOG_TYPE_DEBUG);
			return oam::API_SUCCESS;
		}
		else {
			if (oam.checkLogStatus("/var/log/Calpont/upgrade-infinidb.log1", "ERROR 1045") ) {
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

} //end of namespace


