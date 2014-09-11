/******************************************************************************************
* $Id: processmanager.cpp 2060 2012-12-05 15:48:42Z dhill $
*
******************************************************************************************/

//#define NDEBUG
#include <cassert>

#include "processmanager.h"

using namespace std;
using namespace processmanager;
using namespace messageqcpp;
using namespace oam;
using namespace logging;
using namespace snmpmanager;
using namespace config;

pthread_mutex_t STATUS_LOCK;
pthread_mutex_t THREAD_LOCK;

extern bool runStandby;
extern bool gsharedNothingFlag;
extern string iface_name;
bool gOAMParentModuleFlag;

oam::DeviceNetworkList startdevicenetworklist;

int upgradethreadStatus = oam::API_SUCCESS;
int startsystemthreadStatus = oam::API_SUCCESS;
int startmodulethreadStatus = oam::API_SUCCESS;
bool startsystemthreadStop = false;
bool startsystemthreadRunning = false;
string gdownActiveOAMModule;
vector<string> downModuleList;
bool startFailOver = false;

HeartBeatProcList hbproclist;

namespace processmanager{


/******************************************************************************************
* @brief	Configuration Constructor
*
* purpose:	Configuration Constructor
*
******************************************************************************************/
Configuration::Configuration()
{
	Oam oam;
	oamModuleInfo_t t;
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

		gOAMParentModuleFlag = boost::get<4>(t);
	}
	catch (exception& e) {
		cout << endl << "ProcMgr Construct Error = " << e.what() << endl;
		exit(-1);
	}

}

/******************************************************************************************
* @brief	Configuration Destructor#
*
* purpose:	Configuration 
*
******************************************************************************************/
Configuration::~Configuration()
{
}

/******************************************************************************************
* @brief	getstateInfo
*
* purpose:	Return the module opstate tag
*
******************************************************************************************/
string Configuration::getstateInfo(string moduleName)
{
	return stateInfoList[moduleName];
}


/******************************************************************************************
* @brief	ProcessLog Constructor
*
* purpose:	ProcessLog Constructorname
*
******************************************************************************************/
ProcessLog::ProcessLog()
{
}

/******************************************************************************************
* @brief	ProcessLog Destructor
*
* purpose:	ProcessLog Destructor
*
******************************************************************************************/
ProcessLog::~ProcessLog()
{
}

/******************************************************************************************
* @brief	writeLog
*
* purpose:	Write the message to the log
*
******************************************************************************************/
void ProcessLog::writeLog(const int lineNumber, const string logContent, const LOG_TYPE logType)
{
	LoggingID lid(17);
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
* @brief	writeLog
*
* purpose:	Write the message to the log
*
******************************************************************************************/
void ProcessLog::writeLog(const int lineNumber, const int logContent, const LOG_TYPE logType)
{
	LoggingID lid(17);
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
* @brief	setSysLogData
*
* purpose:	Write the message to the log
*
******************************************************************************************/
 void ProcessLog::setSysLogData()
{
	return;
}

/******************************************************************************************
* @brief	getSysLogData
*
* purpose:	return the sysLogData
*
******************************************************************************************/
string ProcessLog::getSysLogData()
{
	string i;
	return i;
}

/******************************************************************************************
* @brief	writeSystemLog
*
* purpose:	log process status change into system log
*
******************************************************************************************/
void ProcessLog::writeSystemLog()
{
}

/******************************************************************************************
* @brief	ProcessManager Constructor
*
* purpose:	ProcessManager Constructor
*
******************************************************************************************/
ProcessManager::ProcessManager(Configuration &aconfig, ProcessLog &alog):config(aconfig), log(alog)
{
}

/******************************************************************************************
* @brief	ProcessManager Destructor
*
* purpose:	ProcessManager Destructor
*
******************************************************************************************/
ProcessManager::~ProcessManager()
{
}

/******************************************************************************************
* @brief	processMSG
*
* purpose:	Process the received message
*
******************************************************************************************/
//void	ProcessManager::processMSG( messageqcpp::IOSocket fIos, messageqcpp::ByteStream msg)
void processMSG(messageqcpp::IOSocket cfIos)
{
	messageqcpp::IOSocket fIos = cfIos;

	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);

	Oam oam;
	ByteStream::byte msgType;
	ByteStream::byte actionType;
	string target;
	ByteStream::byte graceful;
	ByteStream::byte ackIndicator = 0;
	ByteStream::byte manualFlag;
	ByteStream ackMsg;
	ByteStream::byte status = 0;

	SNMPManager aManager;
	SystemModuleTypeConfig systemmoduletypeconfig;
	SystemProcessConfig systemprocessconfig;

	ByteStream msg;

	try{
		msg = fIos.read();
	}
	catch(...)
	{
		pthread_exit(0);
	}

	if (msg.length() <= 0) {
		fIos.close();
		pthread_exit(0);
	}

	try{
		oam.getSystemConfig(systemmoduletypeconfig);
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

	msg >> msgType;

	startsystemthreadStop = false;

	switch (msgType) {
		case REQUEST:
			msg >> actionType;
			msg >> target;
			msg >> graceful;
			msg >> ackIndicator;
        	msg >> manualFlag;

			switch (actionType) {
				case STOPMODULE:
				{
					uint16_t count, hostConfigCount;
					string value;
					oam::DeviceNetworkConfig devicenetworkconfig;
					oam::DeviceNetworkList devicenetworklist;

					//get module count to remove
					msg >> count;

					if ( count > 0 ) {

						for (int i = 0; i < count; i++)
						{
							msg >> value;	
							devicenetworkconfig.DeviceName = value;
							msg >> value;	
							devicenetworkconfig.UserTempDeviceName = value;
							msg >> value;	
							devicenetworkconfig.DisableState = value;
							devicenetworklist.push_back(devicenetworkconfig);
							msg >> hostConfigCount;
						}
	
						string password;
	
						msg >> password;
	
						DeviceNetworkList::iterator listPT = devicenetworklist.begin();

						for( ; listPT != devicenetworklist.end() ; listPT++)
						{
							string moduleName = (*listPT).DeviceName;

							log.writeLog(__LINE__,  "MSG RECEIVED: Stop Module request on " + moduleName );

							string moduletype = moduleName.substr(0,MAX_MODULE_TYPE_SIZE);
							status = API_SUCCESS;
		
							int opState;
							bool degraded;
							try {
								oam.getModuleStatus(moduleName, opState, degraded);
							}
							catch (exception& ex)
							{
								string error = ex.what();
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": " + error, LOG_TYPE_ERROR);
							}
							catch(...)
							{
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": Caught unknown exception!", LOG_TYPE_ERROR);
							}
		
							if (opState != oam::MAN_DISABLED) {
								status = processManager.stopModule(moduleName, graceful, manualFlag);
								log.writeLog(__LINE__, "Stop Module Completed on " + moduleName, LOG_TYPE_INFO);

								Configuration config;
								if ( moduleName == config.OAMStandbyName() ) {
									string newStandbyModule = processManager.getStandbyModule();
									if ( !newStandbyModule.empty() && newStandbyModule != "NONE")
										processManager.setStandbyModule(newStandbyModule);
									else
									{
										Config* sysConfig = Config::makeConfig();
	
										// clear Standby OAM Module
										sysConfig->setConfig("SystemConfig", "StandbyOAMModuleName", oam::UnassignedName);
										sysConfig->setConfig("ProcStatusControlStandby", "IPAddr", oam::UnassignedIpAddr);
								
										//update Calpont Config table
										try {
											sysConfig->write();
										}
										catch(...)
										{
											log.writeLog(__LINE__, "ERROR: sysConfig->write", LOG_TYPE_ERROR);
										}
									}
								}
							}
							else {
								status = API_DISABLED;
								log.writeLog(__LINE__, "Stop Module requested Ignored on a Disabled " + moduleName);
							}
						}
					}
					else
					{
						status = oam::API_INVALID_PARAMETER;
						log.writeLog(__LINE__, "STOPMODULE: Module Count invalid = " + oam.itoa(count));
					}

					log.writeLog(__LINE__, "STOPMODULE: ACK received from Process-Monitor, return status = " + oam.itoa(status));
					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "STOPMODULE: ACK back to sender");
					}

					break;
				}
				case SHUTDOWNMODULE:
				{
					uint16_t count, hostConfigCount;
					string value;
					oam::DeviceNetworkConfig devicenetworkconfig;
					oam::DeviceNetworkList devicenetworklist;

					//get module count to remove
					msg >> count;

					if ( count > 0 ) {

						for (int i = 0; i < count; i++)
						{
							msg >> value;	
							devicenetworkconfig.DeviceName = value;
							msg >> value;	
							devicenetworkconfig.UserTempDeviceName = value;
							msg >> value;	
							devicenetworkconfig.DisableState = value;
							devicenetworklist.push_back(devicenetworkconfig);
							msg >> hostConfigCount;
						}
	
						string password;
	
						msg >> password;
	
						DeviceNetworkList::iterator listPT = devicenetworklist.begin();

						for( ; listPT != devicenetworklist.end() ; listPT++)
						{
							string moduleName = (*listPT).DeviceName;

							log.writeLog(__LINE__,  "MSG RECEIVED: Shutdown Module request on " + moduleName );

							status = API_SUCCESS;
		
							log.writeLog(__LINE__, "Shutdown Module Requested on " + moduleName, LOG_TYPE_INFO);
							processManager.shutdownModule(moduleName, graceful, manualFlag);

							//check for SIMPLEX Processes on mate might need to be started
							processManager.checkSimplexModule(moduleName);

							Configuration config;
							if ( moduleName == config.OAMStandbyName() ) {
								string newStandbyModule = processManager.getStandbyModule();
								if ( !newStandbyModule.empty() && newStandbyModule != "NONE")
									processManager.setStandbyModule(newStandbyModule);
							}
						}
					}
					else
					{
						status = oam::API_INVALID_PARAMETER;
						log.writeLog(__LINE__, "SHUTDOWNMODULE: Module Count invalid = " + oam.itoa(count));
					}

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "SHUTDOWNMODULE: ACK back to sender, return status = " + oam.itoa(status));
					}

					break;
				}
				case STARTMODULE:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Start Module request" );

					uint16_t count, hostConfigCount;
					string value;
					oam::DeviceNetworkConfig devicenetworkconfig;
					startdevicenetworklist.clear();

					//get module count to remove
					msg >> count;

					if ( count > 0 ) {

						for (int i = 0; i < count; i++)
						{
							msg >> value;	
							devicenetworkconfig.DeviceName = value;
							msg >> value;	
							devicenetworkconfig.UserTempDeviceName = value;
							msg >> value;	
							devicenetworkconfig.DisableState = value;
							startdevicenetworklist.push_back(devicenetworkconfig);
							msg >> hostConfigCount;
						}
	
						string password;
	
						msg >> password;
	
						pthread_t startsystemthread;
						status = pthread_create (&startsystemthread, NULL, (void*(*)(void*)) &startSystemThread, &startdevicenetworklist);

						if ( status != 0 ) {
							log.writeLog(__LINE__, "STARTMODULE: pthread_create failed, return status = " + oam.itoa(status));
							status = API_FAILURE;
						}

						if (status == 0 && ackIndicator)
						{
							pthread_join(startsystemthread, NULL);
							status = startsystemthreadStatus;
						}

						if( status == API_SUCCESS) {
							//distribute config file
							processManager.distributeConfigFile("system");	
		
							processManager.reinitProcessType("ExeMgr");
						}
					}
					else
					{
						status = oam::API_INVALID_PARAMETER;
						log.writeLog(__LINE__, "STARTMODULE: Module Count invalid = " + oam.itoa(count));
					}

					log.writeLog(__LINE__, "STARTMODULE: ACK received from Process-Monitor, return status = " + oam.itoa(status));					

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "STARTMODULE: ACK back to sender");
					}

					break;
				}
				case RESTARTMODULE:
				{
					uint16_t count, hostConfigCount;
					string value;
					oam::DeviceNetworkConfig devicenetworkconfig;
					startdevicenetworklist.clear();

					//get module count to remove
					msg >> count;

					if ( count > 0 ) {

						for (int i = 0; i < count; i++)
						{
							msg >> value;	
							devicenetworkconfig.DeviceName = value;
							msg >> value;	
							devicenetworkconfig.UserTempDeviceName = value;
							msg >> value;	
							devicenetworkconfig.DisableState = value;
							startdevicenetworklist.push_back(devicenetworkconfig);
							msg >> hostConfigCount;
						}
	
						string password;
	
						msg >> password;
	
						DeviceNetworkList::iterator listPT = startdevicenetworklist.begin();

						for( ; listPT != startdevicenetworklist.end() ; listPT++)
						{
							string moduleName = (*listPT).DeviceName;

							log.writeLog(__LINE__,  "MSG RECEIVED: Restart Module request on " + moduleName );
							status = API_SUCCESS;
		
							int opState;
							bool degraded;
							try {
								oam.getModuleStatus(moduleName, opState, degraded);
							}
							catch (exception& ex)
							{
								string error = ex.what();
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": " + error, LOG_TYPE_ERROR);
							}
							catch(...)
							{
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": Caught unknown exception!", LOG_TYPE_ERROR);
							}
		
							if (opState != oam::MAN_DISABLED) {
		
								status = processManager.stopModule(moduleName, graceful, manualFlag);
	
								log.writeLog(__LINE__, "Stop Module Completed on " + moduleName, LOG_TYPE_INFO);

								Configuration config;
								if ( moduleName == config.OAMStandbyName() ) {
									string newStandbyModule = processManager.getStandbyModule();
									if ( !newStandbyModule.empty() && newStandbyModule != "NONE")
										processManager.setStandbyModule(newStandbyModule);
								}
							}
							else {
								status = API_DISABLED;
								log.writeLog(__LINE__, "Stop Module requested Ignored on a Disabled " + moduleName);
							}	
						}

						pthread_t startsystemthread;
						status = pthread_create (&startsystemthread, NULL, (void*(*)(void*)) &startSystemThread, &startdevicenetworklist);
	
						if ( status != 0 ) {
							log.writeLog(__LINE__, "RESTARTMODULE: pthread_create failed, return status = " + oam.itoa(status));
							status = API_FAILURE;
						}

						if (status == 0 && ackIndicator)
						{
							pthread_join(startsystemthread, NULL);
							status = startsystemthreadStatus;
						}

						if( status == API_SUCCESS) {
							//distribute config file
							processManager.distributeConfigFile("system");	

							processManager.reinitProcessType("ExeMgr");
						}
					}
					else
					{
						status = oam::API_INVALID_PARAMETER;
						log.writeLog(__LINE__, "RESTARTMODULE: Module Count invalid = " + oam.itoa(count));
					}

					log.writeLog(__LINE__, "RESTARTMODULE: ACK received from Process-Monitor, return status = " + oam.itoa(status));
					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << (ByteStream::byte) status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "RESTARTMODULE: ACK back to sender");
					}

					break;
				}

				case DISABLEMODULE:
				{
					uint16_t count, hostConfigCount;
					string value;
					oam::DeviceNetworkConfig devicenetworkconfig;
					oam::DeviceNetworkList devicenetworklist;

					//get module count to remove
					msg >> count;

					if ( count > 0 ) {

						for (int i = 0; i < count; i++)
						{
							msg >> value;	
							devicenetworkconfig.DeviceName = value;
							msg >> value;	
							devicenetworkconfig.UserTempDeviceName = value;
							msg >> value;	
							devicenetworkconfig.DisableState = value;
							devicenetworklist.push_back(devicenetworkconfig);
							msg >> hostConfigCount;
						}
	
						string password;
	
						msg >> password;
	
						DeviceNetworkList::iterator listPT = devicenetworklist.begin();

						for( ; listPT != devicenetworklist.end() ; listPT++)
						{
							string moduleName = (*listPT).DeviceName;

							log.writeLog(__LINE__,  "MSG RECEIVED: Disable Module request on " + moduleName );
		
							// check module status, Disable module
							int opState;
							bool degraded;
							try {
								oam.getModuleStatus(moduleName, opState, degraded);
							}
							catch (exception& ex)
							{
								string error = ex.what();
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": " + error, LOG_TYPE_ERROR);
							}
							catch(...)
							{
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": Caught unknown exception!", LOG_TYPE_ERROR);
							}
		
							//don't allow disble of current Parent OAM Module
							if ( moduleName == config.moduleName() )
							{
								log.writeLog(__LINE__,  "ERROR: can't disable Parent OAM module", LOG_TYPE_ERROR);
								status = API_INVALID_PARAMETER;
								break;
							}

							if (opState == oam::MAN_OFFLINE || opState == oam::MAN_DISABLED
									|| opState == oam::MAN_DISABLED ) {
								status = processManager.disableModule(moduleName, true);
								log.writeLog(__LINE__, "Disable Module Completed on " + moduleName, LOG_TYPE_INFO);

								//check for SIMPLEX Processes on mate might need to be started
								processManager.checkSimplexModule(moduleName);
							}
							else
							{
								log.writeLog(__LINE__,  "ERROR: module not stopped", LOG_TYPE_ERROR);
								status = API_FAILURE;
								break;
							}
						}
					}
					else
					{
						status = oam::API_INVALID_PARAMETER;
						log.writeLog(__LINE__, "DISABLEMODULE: Module Count invalid = " + oam.itoa(count));
					}

					log.writeLog(__LINE__, "DISABLEMODULE: ACK received from Process-Monitor, return status = " + oam.itoa(status));

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "DISABLEMODULE: ACK back to sender");
					}

					break;
				}

				case ENABLEMODULE:
				{
					uint16_t count, hostConfigCount;
					string value;
					oam::DeviceNetworkConfig devicenetworkconfig;
					oam::DeviceNetworkList devicenetworklist;

					//get module count to remove
					msg >> count;

					if ( count > 0 ) {

						for (int i = 0; i < count; i++)
						{
							msg >> value;	
							devicenetworkconfig.DeviceName = value;
							msg >> value;	
							devicenetworkconfig.UserTempDeviceName = value;
							msg >> value;	
							devicenetworkconfig.DisableState = value;
							devicenetworklist.push_back(devicenetworkconfig);
							msg >> hostConfigCount;
						}
	
						string password;
	
						msg >> password;
	
						DeviceNetworkList::iterator listPT = devicenetworklist.begin();

						//stopModules being removed with the REMOVE option, which will stop process
						for( ; listPT != devicenetworklist.end() ; listPT++)
						{
							string moduleName = (*listPT).DeviceName;

							log.writeLog(__LINE__,  "MSG RECEIVED: Enable Module request on " + moduleName );
		
							int opState;
							bool degraded;
							try {
								oam.getModuleStatus(moduleName, opState, degraded);
							}
							catch (exception& ex)
							{
								string error = ex.what();
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": " + error, LOG_TYPE_ERROR);
							}
							catch(...)
							{
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": Caught unknown exception!", LOG_TYPE_ERROR);
							}
		
							if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED) {
								status = processManager.enableModule(moduleName, oam::MAN_OFFLINE);
								log.writeLog(__LINE__, "Enable Module Completed on " + moduleName, LOG_TYPE_INFO);
							}
							else
							{
								log.writeLog(__LINE__,  "ERROR: module name not Disabled", LOG_TYPE_ERROR);
								status = API_INVALID_STATE;
								break;
							}
						}
					}
					else
					{
						status = oam::API_INVALID_PARAMETER;
						log.writeLog(__LINE__, "ENABLEMODULE: Module Count invalid = " + oam.itoa(count));
					}

					log.writeLog(__LINE__, "ENABLEMODULE: ACK received from Process-Monitor, return status = " + oam.itoa(status));

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "ENABLEMODULE: ACK back to sender");
					}

					break;
				}

				case STOPSYSTEM:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Stop System request..." );

					//Check whether cpimport is runing
					int checkpid = system( "pidof 'cpimport' > /dev/null" );
					if ( checkpid == 0 )
					{
						log.writeLog(__LINE__, "Stop System Failed: cpimport Active", LOG_TYPE_ERROR);
	
						if (ackIndicator)
						{
							ackMsg << (ByteStream::byte) oam::ACK;
							ackMsg << actionType;
							ackMsg << target;
							ackMsg << (ByteStream::byte) API_CPIMPORT_ACTIVE;
							fIos.write(ackMsg);
	
							log.writeLog(__LINE__, "STOPMODULE: ACK back to sender");
						}
						break;
					}

					//set the flag to have any startsystemthreads to exit out before stop is done
					startsystemthreadStop = true;
					if ( startsystemthreadRunning )
						sleep(5);

					//stop by process type first, if system is ACTIVE
					SystemStatus systemstatus;
					try {
						oam.getSystemStatus(systemstatus);
					}
					catch(...)
					{}

					//set system status
					processManager.setSystemState(oam::MAN_OFFLINE);

					//call to update module status and send notification message
					//stop all of processes..
					for( unsigned int i = 0 ;i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
					{
						int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
						if( moduleCount == 0)
							continue;
			
						DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
						for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
						{
							int opState;
							bool degraded;
							try {
								oam.getModuleStatus((*pt).DeviceName, opState, degraded);
							}
							catch (exception& ex)
							{
								string error = ex.what();
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": " + error, LOG_TYPE_ERROR);
							}
							catch(...)
							{
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": Caught unknown exception!", LOG_TYPE_ERROR);
							}
		
							if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
								continue;

							processManager.stopModule((*pt).DeviceName, STATUS_UPDATE, manualFlag);
						}
					}

					if (systemstatus.SystemOpState == ACTIVE && graceful == oam::GRACEFUL)
						processManager.stopProcessTypes(manualFlag);

					//stop all of processes..
					for( unsigned int i = 0 ;i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
					{
						int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
						if( moduleCount == 0)
							continue;
			
						DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
						for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
						{
							//skip OAM Parent module, do at the end
							if ( (*pt).DeviceName == config.moduleName() )
								continue;

							int opState;
							bool degraded;
							try {
								oam.getModuleStatus((*pt).DeviceName, opState, degraded);
							}
							catch (exception& ex)
							{
								string error = ex.what();
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": " + error, LOG_TYPE_ERROR);
							}
							catch(...)
							{
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": Caught unknown exception!", LOG_TYPE_ERROR);
							}
		
							if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
								continue;

							log.writeLog(__LINE__,  "STOPSYSTEM: Request Stop Module on " + (*pt).DeviceName );

							int retStatus = processManager.stopModule((*pt).DeviceName, graceful, manualFlag);

							log.writeLog(__LINE__, "STOPSYSTEM: ACK received from Process-Monitor, return status = " + oam.itoa(status));
							if (retStatus != API_SUCCESS)
								status = retStatus;
						}
					}

					if ( graceful == INSTALL ) {
						//do stopmodule last since procmon will take down procmgr

						//run save.brm script
						processManager.saveBRM();
	
						log.writeLog(__LINE__, "Stop System Completed", LOG_TYPE_INFO);
	
						if (ackIndicator)
						{
							ackMsg << (ByteStream::byte) oam::ACK;
							ackMsg << actionType;
							ackMsg << target;
							ackMsg << (ByteStream::byte) API_SUCCESS;
							fIos.write(ackMsg);
	
							log.writeLog(__LINE__, "STOPMODULE: ACK back to sender");
						}
	
						startsystemthreadStop = false;
	
						processManager.setSystemState(oam::MAN_OFFLINE);

						//now stop local module
						processManager.stopModule(config.moduleName(), graceful, manualFlag );
					}
					else
					{
						//now stop local module
						processManager.stopModule(config.moduleName(), graceful, manualFlag );
	
						//run save.brm script
						processManager.saveBRM();
	
						log.writeLog(__LINE__, "Stop System Completed", LOG_TYPE_INFO);
	
						if (ackIndicator)
						{
							ackMsg << (ByteStream::byte) oam::ACK;
							ackMsg << actionType;
							ackMsg << target;
							ackMsg << (ByteStream::byte) API_SUCCESS;
							fIos.write(ackMsg);
	
							log.writeLog(__LINE__, "STOPMODULE: ACK back to sender");
						}
	
						startsystemthreadStop = false;
	
						processManager.setSystemState(oam::MAN_OFFLINE);
					}

					break;
				}
				case SHUTDOWNSYSTEM:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Shutdown System request..." );

					//Check whether cpimport is running
					int checkpid = system( "pidof 'cpimport' > /dev/null" );
					if ( checkpid == 0 )
					{
						log.writeLog(__LINE__, "Shutdown System Failed: cpimport Active", LOG_TYPE_ERROR);
	
						if (ackIndicator)
						{
							ackMsg << (ByteStream::byte) oam::ACK;
							ackMsg << actionType;
							ackMsg << target;
							ackMsg << (ByteStream::byte) API_CPIMPORT_ACTIVE;
							fIos.write(ackMsg);
	
							log.writeLog(__LINE__, "SHUTDOWNSYSTEM: ACK back to sender");
						}
						break;
					}

					//get system status
					SystemStatus systemstatus;
					try {
						oam.getSystemStatus(systemstatus);
					}
					catch(...)
					{}

					//call to update module status and send notification message
					//stop all of processes..
					for( unsigned int i = 0 ;i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
					{
						int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
						if( moduleCount == 0)
							continue;
			
						DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
						for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
						{
							int opState;
							bool degraded;
							try {
								oam.getModuleStatus((*pt).DeviceName, opState, degraded);
							}
							catch (exception& ex)
							{
								string error = ex.what();
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": " + error, LOG_TYPE_ERROR);
							}
							catch(...)
							{
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": Caught unknown exception!", LOG_TYPE_ERROR);
							}
		
							if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
								continue;

							processManager.stopModule((*pt).DeviceName, STATUS_UPDATE, manualFlag);
						}
					}

					//stop by process type first, if system is ACTIVE
					if (systemstatus.SystemOpState == ACTIVE)
						processManager.stopProcessTypes(manualFlag);

					int retStatus;
					for( unsigned int i = 0 ;i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
					{
						int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
						if( moduleCount == 0)
							continue;
			
						DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
						for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
						{
							//do local module last
							if ( (*pt).DeviceName == config.moduleName() )
								continue;

							int opState;
							bool degraded;
							try {
								oam.getModuleStatus((*pt).DeviceName, opState, degraded);
							}
							catch (exception& ex)
							{
								string error = ex.what();
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": " + error, LOG_TYPE_ERROR);
							}
							catch(...)
							{
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": Caught unknown exception!", LOG_TYPE_ERROR);
							}
		
							if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
								continue;

							if ( (*pt).DeviceName.find("xm",0) != 0 ) {
								log.writeLog(__LINE__,  "SHUTDOWNSYSTEM: Request Shutdown Module on " + (*pt).DeviceName );

								retStatus = processManager.shutdownModule((*pt).DeviceName, graceful, manualFlag);

								log.writeLog(__LINE__, "SHUTDOWNSYSTEM: ACK received from Process-Monitor, return status = " + oam.itoa(status));
							}
							else { //stopModule for xm
								log.writeLog(__LINE__,  "SHUTDOWNSYSTEM: Request Stop Module on " + (*pt).DeviceName );
								retStatus = processManager.stopModule((*pt).DeviceName, graceful, manualFlag);
								log.writeLog(__LINE__, "SHUTDOWNSYSTEM: ACK received from Process-Monitor, return status = " + oam.itoa(status));
							}
							if (retStatus != API_SUCCESS)
								status = retStatus;
						}
					}

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) API_SUCCESS;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "SHUTDOWNSYSTEM: ACK back to sender, return status = " + oam.itoa(API_SUCCESS));
					}

					Config* sysConfig = Config::makeConfig();

					// clear Standby OAM Module
					sysConfig->setConfig("SystemConfig", "StandbyOAMModuleName", oam::UnassignedName);
					sysConfig->setConfig("ProcStatusControlStandby", "IPAddr", oam::UnassignedIpAddr);
			
					//update Calpont Config table
					try {
						sysConfig->write();
					}
					catch(...)
					{
						log.writeLog(__LINE__, "ERROR: sysConfig->write", LOG_TYPE_ERROR);
					}

					log.writeLog(__LINE__, "Shutdown System Completed", LOG_TYPE_INFO);

					// now do local module
					log.writeLog(__LINE__,  "SHUTDOWNSYSTEM: Request Shutdown Module on " + config.moduleName() );

					retStatus = processManager.shutdownModule(config.moduleName(), graceful, manualFlag);

					if (retStatus != API_SUCCESS)
						status = retStatus;

					log.writeLog(__LINE__, "SHUTDOWNSYSTEM: ACK received from Process-Monitor, return status = " + oam.itoa(status));

					//run save.brm script
					processManager.saveBRM();

					break;
				}
				case STARTSYSTEM:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Start System request..." );

					// get system status and don't process if already in-process
					try {
						SystemStatus systemstatus;
						oam.getSystemStatus(systemstatus);
				
						if (systemstatus.SystemOpState == MAN_INIT) {
							log.writeLog(__LINE__, "STARTSYSTEM: Start already in-progess");

							if (ackIndicator)
							{
								ackMsg << (ByteStream::byte) oam::ACK;
								ackMsg << actionType;
								ackMsg << target;
								ackMsg << (ByteStream::byte) API_ALREADY_IN_PROGRESS;
								fIos.write(ackMsg);
		
								log.writeLog(__LINE__, "STARTSYSTEM: ACK back to sender");
							}
		
							break;
						}
					}
					catch (exception& ex)
					{
						string error = ex.what();
						log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemStatus: " + error, LOG_TYPE_ERROR);
					}
					catch(...)
					{
						log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemStatus: Caught unknown exception!", LOG_TYPE_ERROR);
					}

					//distribute config file
					processManager.distributeConfigFile("system");	

					oam::DeviceNetworkList devicenetworklist;
					pthread_t startsystemthread;
					status = pthread_create (&startsystemthread, NULL, (void*(*)(void*)) &startSystemThread, &devicenetworklist);

					if ( status != 0 ) {
						log.writeLog(__LINE__, "STARTSYSTEMS: pthread_create failed, return status = " + oam.itoa(status));
						status = API_FAILURE;
					}

					if (status == 0 && ackIndicator)
					{
						pthread_join(startsystemthread, NULL);
						status = startsystemthreadStatus;

						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "STARTSYSTEM: ACK back to sender");
					}

					log.writeLog(__LINE__, "STARTSYSTEM: Start System Request Completed with status = " + oam.itoa(status));

					break;
				}
				case RESTARTSYSTEM:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Restart System request..." );

					//Check whether cpimport is runing
					int checkpid = system( "pidof 'cpimport' > /dev/null" );
					if ( checkpid == 0 )
					{
						log.writeLog(__LINE__, "Restart System Failed: cpimport Active", LOG_TYPE_ERROR);
	
						if (ackIndicator)
						{
							ackMsg << (ByteStream::byte) oam::ACK;
							ackMsg << actionType;
							ackMsg << target;
							ackMsg << (ByteStream::byte) API_CPIMPORT_ACTIVE;
							fIos.write(ackMsg);
	
							log.writeLog(__LINE__, "RESTARTSYSTEM: ACK back to sender");
						}
						break;
					}

					//set the flag to have any startsystemthreads to exit out before stop is done
					startsystemthreadStop = true;
					if ( startsystemthreadRunning )
						sleep(5);

					//get system status
					SystemStatus systemstatus;
					try {
						oam.getSystemStatus(systemstatus);
					}
					catch(...)
					{}

					//set system status
					processManager.setSystemState(oam::MAN_OFFLINE);

					//call to update module status and send notification message
					//stop all of processes..
					for( unsigned int i = 0 ;i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
					{
						int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
						if( moduleCount == 0)
							continue;
			
						DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
						for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
						{
							int opState;
							bool degraded;
							try {
								oam.getModuleStatus((*pt).DeviceName, opState, degraded);
							}
							catch (exception& ex)
							{
								string error = ex.what();
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": " + error, LOG_TYPE_ERROR);
							}
							catch(...)
							{
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": Caught unknown exception!", LOG_TYPE_ERROR);
							}
		
							if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
								continue;

							processManager.stopModule((*pt).DeviceName, STATUS_UPDATE, manualFlag);
						}
					}

					//stop by process type first, if system is ACTIVE
					if (systemstatus.SystemOpState == ACTIVE)
						processManager.stopProcessTypes(manualFlag);

					status = API_SUCCESS;

					// stop modules
					for( unsigned int i = 0 ;i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
					{
						int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
						if( moduleCount == 0)
							continue;
			
						DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
						for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
						{
							//skip OAM Parent module, do at the end
							if ( (*pt).DeviceName == config.moduleName() )
								continue;

							int opState;
							bool degraded;
							try {
								oam.getModuleStatus((*pt).DeviceName, opState, degraded);
							}
							catch (exception& ex)
							{
								string error = ex.what();
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": " + error, LOG_TYPE_ERROR);
							}
							catch(...)
							{
								log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": Caught unknown exception!", LOG_TYPE_ERROR);
							}
		
							if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
								continue;

							log.writeLog(__LINE__,  "RESTARTSYSTEM: Request Stop Module on " + (*pt).DeviceName );

							int retStatus = processManager.stopModule((*pt).DeviceName, graceful, manualFlag);

							log.writeLog(__LINE__, "RESTARTSYSTEM: ACK received from Process-Monitor, return status = " + oam.itoa(status));
							if (retStatus != API_SUCCESS)
								status = retStatus;
						}
					}
					//now stop local module
					processManager.stopModule(config.moduleName(), graceful, manualFlag );

					//run save.brm script
					processManager.saveBRM();

					log.writeLog(__LINE__, "RESTARTSYSTEM: ACK received from Process-Monitor for stopModule requests, return status = " + oam.itoa(status));

					startsystemthreadStop = false;

					if (status == API_SUCCESS ) {
						//distribute config file
						processManager.distributeConfigFile("system");	

						oam::DeviceNetworkList devicenetworklist;
						pthread_t startsystemthread;
						pthread_create (&startsystemthread, NULL, (void*(*)(void*)) &startSystemThread, &devicenetworklist);

						if ( status != 0 ) {
							log.writeLog(__LINE__, "STARTMODULE: pthread_create failed, return status = " + oam.itoa(status));
							status = API_FAILURE;
						}

						if (status == 0 && ackIndicator)
						{
							pthread_join(startsystemthread, NULL);
							status = startsystemthreadStatus;
						}

						log.writeLog(__LINE__, "RESTARTSYSTEM: Start System Request Completed", LOG_TYPE_INFO);
					}
	
					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "RESTARTSYSTEM: ACK back to sender");
					}

					log.writeLog(__LINE__, "Restart System Completed, status = " + oam.itoa(status), LOG_TYPE_INFO);

					break;
				}
				case STOPPROCESS:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Stop Process request on " + target );
					string moduleName;

					msg >> moduleName;
					status = API_SUCCESS;

					status = processManager.stopProcess(moduleName, target, graceful, manualFlag);

					log.writeLog(__LINE__, "STOPPROCESS: ACK received from Process-Monitor, return status = " + oam.itoa(status));
					log.writeLog(__LINE__, "Stop Process Completed on " + moduleName + " / " + target, LOG_TYPE_INFO );

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "STOPPROCESS: ACK back to sender");
					}
					break;	
				}
				case STARTPROCESS:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Start Process request on " + target);
					string moduleName;

					msg >> moduleName;

					status = processManager.startProcess(moduleName, target, graceful);

					log.writeLog(__LINE__, "STARTPROCESS: ACK received from Process-Monitor, return status = " + oam.itoa(status));
					log.writeLog(__LINE__, "Start Process Completed on " + moduleName + " / " + target, LOG_TYPE_INFO );

					// if a PrimProc was restarted, restart ACTIVE ExeMgr(s) and DDL/DMLProc
					if( target.find("PrimProc") == 0) {

						//distribute config file
						processManager.distributeConfigFile("system");	

						processManager.reinitProcessType("ExeMgr");
						processManager.reinitProcessType("DDLProc");
						processManager.reinitProcessType("DMLProc");
					}

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "STARTPROCESS: ACK back to sender");
					}
					break;	
				}
				case RESTARTPROCESS:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Restart Process request on " + target );
					string moduleName;

					msg >> moduleName;

					status = processManager.restartProcess(moduleName, target, graceful, manualFlag);

					// if a PrimProc was restarted, restart ACTIVE ExeMgr(s)
					if( target.find("PrimProc") == 0) {

						//distribute config file
						processManager.distributeConfigFile("system");	

						processManager.reinitProcessType("ExeMgr");
						processManager.reinitProcessType("DDLProc");
						processManager.reinitProcessType("DMLProc");
					}

					log.writeLog(__LINE__, "RESTARTPROCESS: ACK received from Process-Monitor, return status = " + oam.itoa(status));
					log.writeLog(__LINE__, "Restart Process Completed on " + moduleName + " / " + target, LOG_TYPE_INFO );

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "RESTARTPROCESS: ACK back to sender");
					}
					break;	
				}
				case UPDATELOG:
				{
					string action;
					string level;

					msg >> action;
					msg >> level;

					log.writeLog(__LINE__,  "MSG RECEIVED: " + action + " logging on " + target + " for level " + level );

					status = API_SUCCESS;
 
					if ( target == "system" ) {
						// send logging message to all modules
						for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
						{
							int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
							if( moduleCount == 0)
								continue;
				
							DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
							for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
							{
								int retStatus = processManager.updateLog(action, (*pt).DeviceName, level);
								if ( retStatus != API_SUCCESS)
									status = retStatus;
							}
						}
					}
					else
					{ // for a specific module
						// validate module name
						bool found = false;
						for( unsigned int i = 0; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
						{
							int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
							if( moduleCount == 0)
								continue;
				
							DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
							for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
							{
								if ((*pt).DeviceName == target) {
									status = processManager.updateLog(action, target, level);
									found = true;
									break;
								}
							}
						}
						if ( found == false ) {
							log.writeLog(__LINE__,  "ERROR: Invalid module name: " +  target, LOG_TYPE_ERROR);
							status = API_INVALID_PARAMETER;
						}
					}

					ackMsg << (ByteStream::byte) oam::ACK;
					ackMsg << actionType;
					ackMsg << target;
					ackMsg << (ByteStream::byte) status;
					fIos.write(ackMsg);

					log.writeLog(__LINE__, "UPDATELOG: ACK back to sender, return status = " + oam.itoa(status));

					break;
				}
				case GETCONFIGLOG:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Get Log Configuation" );

					status = API_SUCCESS;
 
					// validate module name and make request
					bool found = false;
					for( unsigned int i = 0; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
					{
						int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
						if( moduleCount == 0)
							continue;
			
						DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
						for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
						{
							if ((*pt).DeviceName == target) {
								status = processManager.getConfigLog(target);
								found = true;
								break;
							}
						}
					}
					if ( found == false ) {
						log.writeLog(__LINE__,  "ERROR: Invalid module name: " +  target, LOG_TYPE_ERROR);
						status = API_INVALID_PARAMETER;
					}

					ackMsg << (ByteStream::byte) oam::ACK;
					ackMsg << actionType;
					ackMsg << target;
					ackMsg << (ByteStream::byte) status;
					fIos.write(ackMsg);

					log.writeLog(__LINE__, "GETCONFIGLOG: ACK back to sender, return status = " + oam.itoa(status));

					break;
				}
				case REINITPROCESS:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Re-Init Process request..." );
					string moduleName;

					msg >> moduleName;

					//distribute config file
					processManager.distributeConfigFile(moduleName);	

					status = processManager.reinitProcess(moduleName, target);

					log.writeLog(__LINE__, "REINITPROCESS: ACK received from Process-Monitor, return status = " + oam.itoa(status));

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "REINITPROCESS: ACK back to sender");
					}
					break;
				}
				case UPDATECONFIG:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Update Process Configuation" );

					status = API_SUCCESS;
 
					//distribute update of process config file
					processManager.distributeConfigFile("system", "ProcessConfig.xml");

					for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
					{
						int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
						if( moduleCount == 0)
							continue;
			
						DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
						for (;pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
						{
							int retStatus = processManager.updateConfig((*pt).DeviceName);
							if (retStatus != API_SUCCESS)
								status = retStatus;
						}
					}

					log.writeLog(__LINE__, "UPDATECONFIG: ACK back to sender, return status = " + oam.itoa(status));
					break;
				}
				case BUILDSYSTEMTABLES:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Send Build System Table request to " + target);

					status = processManager.buildSystemTables(target);

					log.writeLog(__LINE__, "BUILDSYSTEMTABLES: ACK received from Process-Monitor, return status = " + oam.itoa(status));

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "BUILDSYSTEMTABLES: ACK back to sender");
					}
					break;
				}
				case ADDMODULE:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Add Module request");

					string value;
					uint16_t count,ivalue,nicCount;
					oam::DeviceNetworkConfig devicenetworkconfig;
					oam::DeviceNetworkList devicenetworklist;
					oam::HostConfig hostconfig;

					//get module count to add
					msg >> count;

					if ( count > 0 ) {

						for (int i = 0; i < count; i++)
						{
							msg >> value;	
							devicenetworkconfig.DeviceName = value;
							msg >> value;	
							devicenetworkconfig.UserTempDeviceName = value;
							msg >> value;	
							devicenetworkconfig.DisableState = value;

							msg >> nicCount;
							for (int j = 0 ; j < nicCount ; j ++ )
							{
								msg >> value;	
								hostconfig.IPAddr = value;
								msg >> value;
								hostconfig.HostName = value;
								msg >> ivalue;
								hostconfig.NicID = ivalue; 
								devicenetworkconfig.hostConfigList.push_back(hostconfig);
							}
							devicenetworklist.push_back(devicenetworkconfig);
							devicenetworkconfig.hostConfigList.clear();
						}
	
						string password;
	
						msg >> password;
	
						status = processManager.addModule(devicenetworklist, password);
	
						log.writeLog(__LINE__, "ADDMODULE: ACK received from Process-Monitor, return status = " + oam.itoa(status));
					}
					else
					{
						status = oam::API_INVALID_PARAMETER;
						log.writeLog(__LINE__, "ADDMODULE: Module Count invalid = " + oam.itoa(count));
					}

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "ADDMODULE: ACK back to sender");
					}

					break;
				}
				case REMOVEMODULE:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Remove Module request");

					uint16_t count, hostConfigCount;
					string value;
					oam::DeviceNetworkConfig devicenetworkconfig;
					oam::DeviceNetworkList devicenetworklist;

					//get module count to remove
					msg >> count;

					if ( count > 0 ) {

						for (int i = 0; i < count; i++)
						{
							msg >> value;	
							devicenetworkconfig.DeviceName = value;
							msg >> value;	
							devicenetworkconfig.UserTempDeviceName = value;
							msg >> value;	
							devicenetworkconfig.DisableState = value;
							devicenetworklist.push_back(devicenetworkconfig);
							msg >> hostConfigCount;
						}
	
						string password;
	
						msg >> password;
	
						status = processManager.removeModule(devicenetworklist);
	
						log.writeLog(__LINE__, "REMOVEMODULE: ACK received from Process-Monitor, return status = " + oam.itoa(status));
						log.writeLog(__LINE__, "Remove Module Completed", LOG_TYPE_INFO);
					}
					else
					{
						status = oam::API_INVALID_PARAMETER;
						log.writeLog(__LINE__, "REMOVEMODULE: Module Count invalid = " + oam.itoa(count));
					}

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "REMOVEMODULE: ACK back to sender");
					}

					break;
				}
				case RECONFIGUREMODULE:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Reconfigure Module request");

					string value;
					uint16_t count,ivalue,nicCount;
					oam::DeviceNetworkConfig devicenetworkconfig;
					oam::DeviceNetworkList devicenetworklist;
					oam::HostConfig hostconfig;

					//get module count
					msg >> count;

					if ( count > 0 ) {

						for (int i = 0; i < count; i++)
						{
							msg >> value;	
							devicenetworkconfig.DeviceName = value;
							msg >> value;	
							devicenetworkconfig.UserTempDeviceName = value;
							msg >> value;	
							devicenetworkconfig.DisableState = value;
	
							msg >> nicCount;
							for (int j = 0 ; j < nicCount ; j ++ )
							{
								msg >> value;	
								hostconfig.IPAddr = value;
								msg >> value;
								hostconfig.HostName = value;
								msg >> ivalue;
								hostconfig.NicID = ivalue; 
								devicenetworkconfig.hostConfigList.push_back(hostconfig);
							}
							devicenetworklist.push_back(devicenetworkconfig);
							devicenetworkconfig.hostConfigList.clear();
						}
	
						string password;
	
						msg >> password;
	
						status = processManager.reconfigureModule(devicenetworklist);
	
						log.writeLog(__LINE__, "RECONFIGUREMODULE: ACK received from Process-Monitor, return status = " + oam.itoa(status));
					}
					else
					{
						status = oam::API_INVALID_PARAMETER;
						log.writeLog(__LINE__, "RECONFIGUREMODULE: Module Count invalid = " + oam.itoa(count));
					}

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << status;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "RECONFIGUREMODULE: ACK back to sender");
					}
					break;
				}
				case STOPPROCESSTYPE:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Stop Process Type request: " + target);

					if ( target == "DBRM" ) {
						processManager.stopProcessType("DBRMControllerNode");
						processManager.stopProcessType("DBRMWorkerNode");
					}
					else
						processManager.stopProcessType(target);

					log.writeLog(__LINE__, "Stop Process Type Completed", LOG_TYPE_INFO );
					break;	
				}
				case STARTPROCESSTYPE:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Start Process Type request: " + target);

					if ( target == "DBRM" ) {
						processManager.startProcessType("DBRMControllerNode");
						processManager.startProcessType("DBRMWorkerNode");
					}
					else
						processManager.startProcessType(target);

					// if a PrimProc was restarted, restart ACTIVE ExeMgr(s) and DDL/DMLProc
					if( target == "PrimProc" ) {

						//distribute config file
						processManager.distributeConfigFile("system");	

						processManager.reinitProcessType("ExeMgr");
						processManager.reinitProcessType("DDLProc");
						processManager.reinitProcessType("DMLProc");
					}

					log.writeLog(__LINE__, "Start Process Type Completed", LOG_TYPE_INFO );
					break;	
				}
				case RESTARTPROCESSTYPE:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Restart Process Type request: " + target);

					if ( target == "DBRM" ) {
						processManager.restartProcessType("DBRMControllerNode");
						processManager.restartProcessType("DBRMWorkerNode");
					}
					else {
						processManager.restartProcessType(target);

						// if a PrimProc was restarted, restart ACTIVE ExeMgr(s) and DDL/DMLProc
						if( target == "PrimProc" ) {

							//distribute config file
							processManager.distributeConfigFile("system");	

							processManager.reinitProcessType("ExeMgr");
							processManager.reinitProcessType("DDLProc");
							processManager.reinitProcessType("DMLProc");
						}
					}

					log.writeLog(__LINE__, "Restart Process Type Completed", LOG_TYPE_INFO );
					break;	
				}
				case REINITPROCESSTYPE:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Reinit Process Type request: " + target);

					status = processManager.reinitProcessType(target);

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) status;
						fIos.write(ackMsg);
					}

					log.writeLog(__LINE__, "Reinit Process Type Completed, return status = " + oam.itoa(status));
					break;	
				}

				case DISTRIBUTECONFIG:
				{
					string file;

					msg >> file;
	
					log.writeLog(__LINE__,  "MSG RECEIVED: Distribute Config File " + target + "/" + file);

					processManager.distributeConfigFile(target, file);
	
					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) oam::API_SUCCESS;
						fIos.write(ackMsg);
					}

					log.writeLog(__LINE__, "Distribute Config File Completed " + target + "/" + file);
					break;	
				}
	
				case SWITCHOAMPARENT:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Switch OAM Parent to : " + target);
	
					status = processManager.switchParentOAMModule(target);
	
					log.writeLog(__LINE__, "Switch OAM Parent Completed", LOG_TYPE_INFO );

					ackMsg << (ByteStream::byte) oam::ACK;
					ackMsg << actionType;
					ackMsg << target;
					ackMsg << (ByteStream::byte) status;
					fIos.write(ackMsg);

					// stop myself
					processManager.stopProcess(config.moduleName(), "ProcessManager", oam::FORCEFUL, true);

					break;	
				}

				default:
					log.writeLog(__LINE__,  "MSG RECEIVED: Invalid type" );
					break;
			}
			break;

			case HEARTBEAT_REGISTER:
			{
				string moduleName;
				string processName;
				ByteStream::byte id;

				msg >> moduleName;
				msg >> processName;
				msg >> id;
				
				HeartBeatProc hbproc;
				hbproc.ModuleName = moduleName;
				hbproc.ProcessName = processName;
				hbproc.ID = id;
				hbproc.receiveFlag = true;

				HeartBeatProcList::iterator list = hbproclist.begin();
				for( ; list != hbproclist.end() ; list++) 
				{
					if ( (*list).ModuleName == moduleName 
							&& (*list).ProcessName == processName 
							&& (*list).ID == id) {
						// already in the list
						break;
					}
				}
				if ( list == hbproclist.end() ) {
				// add to list
					hbproclist.push_front(hbproc);
					log.writeLog(__LINE__, "Adding Process to Heartbeat Monitor list: " + moduleName + " / " + processName + " / " + oam.itoa(id));
				}
			}
			break;

			case HEARTBEAT_DEREGISTER:
			{
				string moduleName;
				string processName;
				ByteStream::byte id;

				msg >> moduleName;
				msg >> processName;
				msg >> id;
	
				HeartBeatProcList::iterator list = hbproclist.begin();
				for( ; list != hbproclist.end() ; list++) 
				{
					if ( (*list).ModuleName == moduleName 
							&& (*list).ProcessName == processName 
							&& (*list).ID == id) {
						hbproclist.erase(list);
						log.writeLog(__LINE__, "Removing Process from Heartbeat Monitor list: " + moduleName + " / " + processName+ " / " + oam.itoa(id));
						break;
					}
				}
			}
			break;

			case HEARTBEAT_SEND:
			{
				string moduleName;
				string processName;
				string timeStamp;
				ByteStream::byte id;
		        ByteStream::byte ackFlag;

				msg >> moduleName;
				msg >> processName;
				msg >> timeStamp;
				msg >> id;
		        msg >> ackFlag;

				if ( ackFlag == oam::ACK_YES ) {
					// send back an ack msg
					ackMsg << (ByteStream::byte) HEARTBEAT_SEND;
					fIos.write(ackMsg);

//log.writeLog(__LINE__, "Heartbeat Ack message sent", LOG_TYPE_DEBUG);
				}

				HeartBeatProcList::iterator list = hbproclist.begin();
				for( ; list != hbproclist.end() ; list++) 
				{
					if ( (*list).ModuleName == moduleName 
							&& (*list).ProcessName == processName 
							&& (*list).ID == id) {
						(*list).receiveFlag = true;
//log.writeLog(__LINE__, "Heartbeat Received: " + moduleName + " / " + processName + " / " + oam.itoa(id) + ", timestamp: " + timeStamp, LOG_TYPE_DEBUG);
						break;
					}
				}
				if ( list == hbproclist.end() ) {
				// not found, add to list
					HeartBeatProc hbproc;
					hbproc.ModuleName = moduleName;
					hbproc.ProcessName = processName;
					hbproc.ID = id;
					hbproc.receiveFlag = true;
					hbproclist.push_front(hbproc);
					log.writeLog(__LINE__, "Adding Process to Heartbeat Monitor list: " + moduleName + " / " + processName + " / " + oam.itoa(id));
				}
			}
			break;

			case PROCESSRESTART:
			{
				string moduleName;
				string processName;
				ByteStream::byte manual;

				msg >> moduleName;
				msg >> processName;
				msg >> manual;

				log.writeLog(__LINE__,  "MSG RECEIVED: Process Restarted on " + moduleName + "/" + processName);

				//request reinit after Process is active
				for ( int i = 0; i < 600 ; i++ ) {
					try {
						ProcessStatus procstat;
						oam.getProcessStatus(processName, moduleName, procstat);

						if (procstat.ProcessOpState == oam::ACTIVE) {
							// if a PrimProc was restarted, reinit ACTIVE ExeMgr(s) and DDL/DMLProc
							if( processName == "PrimProc") {
			
								//distribute config file
								processManager.distributeConfigFile("system");
			
								processManager.reinitProcessType("ExeMgr");
								processManager.reinitProcessType("DDLProc");
								processManager.reinitProcessType("DMLProc");
							}

							// if a ControllerNode was restarted, restart DMLProc
							if( processName == "DBRMControllerNode") {
							//	sleep(5);
							//	processManager.reinitProcessType("DBRMWorkerNode");

								processManager.restartProcessType("DMLProc");
							}

							// if a DDLProc was restarted, reinit DMLProc
							if( processName == "DDLProc") {
								processManager.restartProcessType("DMLProc");
							}

							//only run on auto process restart
							if (manual == 0 )
							{
								//get dbhealth flag
								string DBHealthMonitorFlag = "n";
								try {
									oam.getSystemConfig( "DBHealthMonitorFlag", DBHealthMonitorFlag);
								}
								catch(...) {
									DBHealthMonitorFlag = "n";
								}
	
								//check the db health
								if (DBHealthMonitorFlag == "y" ) {
									log.writeLog(__LINE__, "Call the check DB Health API", LOG_TYPE_DEBUG);
									try {
										oam.checkDBHealth();
										log.writeLog(__LINE__, "check DB Health passed", LOG_TYPE_DEBUG);
									}
									catch(...)
									{
										log.writeLog(__LINE__, "check DB Health FAILED", LOG_TYPE_ERROR);
									}
								}
							}

							break;
						}
						sleep(1);
					}
					catch (exception& ex)
					{
						string error = ex.what();
						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
						break;
					}
					catch(...)
					{
						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
						break;
					}
				}
			}
			break;

			case GETDBRMDATA:
			{
				log.writeLog(__LINE__,  "MSG RECEIVED: Get DBRM Data Files");

				int ret = processManager.getDBRMData(fIos);

				if ( ret == oam::API_SUCCESS )
					log.writeLog(__LINE__, "Get DBRM Data Files Completed");
				else
					log.writeLog(__LINE__, "Get DBRM Data Files Failed");
			}
			break;

			case GETALARMDATA:
			{
				log.writeLog(__LINE__,  "MSG RECEIVED: Get Alarm Data Files");

				string date;

				msg >> date;

				processManager.getAlarmData(fIos, GETALARMDATA, date);

				log.writeLog(__LINE__, "Get Alarm Data Files Completed");
			}
			break;

			case GETACTIVEALARMDATA:
			{
//				log.writeLog(__LINE__,  "MSG RECEIVED: Get Active Alarm Data Files");

				//pull off, but don't need
				string date;

				msg >> date;

				processManager.getAlarmData(fIos, GETACTIVEALARMDATA, "");

//				log.writeLog(__LINE__, "Get Active Alarm Data Files Completed");
			}
			break;

			default:
				break;
	}
	
	fIos.close();
	pthread_exit(0);
}

/******************************************************************************************
* @brief	getAlarmData
*
* purpose:	get DBRM Data and send to requester
*
******************************************************************************************/
int ProcessManager::getAlarmData(messageqcpp::IOSocket fIos, int type, std::string date)
{
	ByteStream msg;
	Oam oam;
	int returnStatus = oam::API_SUCCESS;

	AlarmList alarmList;

	if ( type == GETALARMDATA ) {
		try {
			SNMPManager sm;
			sm.getAlarm(date, alarmList);
		}
		catch(...)
		{
			msg << (ByteStream::byte) oam::ACK;
			msg << (ByteStream::byte) type;
			msg << (ByteStream::byte) oam::API_FAILURE;
			fIos.write(msg);
		
			return oam::API_FAILURE;
		}
	}
	else
	{
		try {
	        SNMPManager sm;
			sm.getActiveAlarm(alarmList);
		}
		catch(...)
		{
			msg << (ByteStream::byte) oam::ACK;
			msg << (ByteStream::byte) type;
			msg << (ByteStream::byte) oam::API_FAILURE;
			fIos.write(msg);
		
			return oam::API_FAILURE;
		}
	}

	msg << (ByteStream::byte) oam::ACK;
	msg << (ByteStream::byte) type;
	msg << (ByteStream::byte) oam::API_SUCCESS;

	//number of alarms
	msg << (ByteStream::byte) alarmList.size();
//log.writeLog(__LINE__, oam.itoa(alarmList.size()), LOG_TYPE_ERROR );

	AlarmList :: iterator i;
	for (i = alarmList.begin(); i != alarmList.end(); ++i)
	{
		msg << (ByteStream::doublebyte) i->second.getAlarmID();
//log.writeLog(__LINE__, oam.itoa(i->second.getAlarmID()), LOG_TYPE_ERROR );
		msg << i->second.getDesc();
		msg << (ByteStream::doublebyte) i->second.getSeverity();
		msg << i->second.getTimestamp();
		msg << i->second.getSname();
		msg << i->second.getPname();
		msg << i->second.getComponentID();
	}

	fIos.write(msg);

	return returnStatus;
}

/******************************************************************************************
* @brief	buildRequestMessage
*
* purpose:	Build a request message
*
******************************************************************************************/
ByteStream  ProcessManager::buildRequestMessage(ByteStream::byte requestID, 
				ByteStream::byte actionIndicator, string processName, bool manualFlag)
{
	ByteStream msg;
	ByteStream::byte messageType = REQUEST;

	msg << messageType;
	msg << requestID;
	msg << actionIndicator;
	if (processName != "" )
		msg << processName;
	msg << (ByteStream::byte) manualFlag;

	return msg;
}

/******************************************************************************************
* @brief	startModule
*
* purpose:	Start all processes on the specified module
*
******************************************************************************************/
int ProcessManager::startModule(string target, messageqcpp::ByteStream::byte actionIndicator, uint16_t startType, bool systemStart)
{
	ByteStream msg;
	ByteStream::byte requestID = STARTALL;
	string processName = "";
	Oam oam;

	if ( startType ==  oam::MAN_OFFLINE )
		setModuleState(target, oam::MAN_INIT);
	else
		setModuleState(target, oam::AUTO_INIT);

	msg = buildRequestMessage(requestID, actionIndicator, processName);

	int returnStatus = sendMsgProcMon( target, msg, requestID );

	if ( returnStatus == API_SUCCESS)
	{
		setModuleState(target, oam::ACTIVE);
	
		//clear alarm, log the event 
		log.writeLog(__LINE__, target + " module is started by request.", LOG_TYPE_DEBUG);
	
		//clear an alarm 
		SNMPManager aManager;
		aManager.sendAlarmReport(target.c_str(), MODULE_DOWN_MANUAL, CLEAR);
		aManager.sendAlarmReport(target.c_str(), MODULE_DOWN_AUTO, CLEAR);

		//update if DDL/DML IPs if local module
		if ( target == config.moduleName() ) {
			setPMProcIPs(target);

			//distribute config file
			distributeConfigFile("system");	
		}
	}
	else
	{
		if ( returnStatus == oam::API_FAILURE || returnStatus == API_FAILURE_DB_ERROR)
			setModuleState(target, oam::FAILED);
		else
			if ( !systemStart )
				setModuleState(target, oam::FAILED);
	
		//log the event 
		log.writeLog(__LINE__, target + " module failed to start!!", LOG_TYPE_DEBUG);
	}

	return returnStatus;
}

/******************************************************************************************
* @brief	stopModule
*
* purpose:	Stop all processes on the specified module
*
******************************************************************************************/
int ProcessManager::stopModule(string target, ByteStream::byte actionIndicator, bool manualFlag)
{
	Configuration config;
	ProcessManager processManager(config, log);
	ByteStream msg;
	ByteStream::byte requestID = STOPALL;
	string processName = "";

	msg = buildRequestMessage(requestID, actionIndicator, processName, manualFlag);

	string msgPort = target;
	msgPort = msgPort + "_ProcessMonitor";

	int returnStatus = API_FAILURE;

	if ( actionIndicator == INSTALL && target == config.OAMParentName() ) {
		// Process Manager will be taken down, do your updates now
		log.writeLog(__LINE__, target + " module is stopped by request.", LOG_TYPE_DEBUG);
	
		if ( manualFlag ) {
			setModuleState(target, oam::MAN_OFFLINE);
		
			//Issue an alarm 
			SNMPManager aManager;				
			aManager.sendAlarmReport(target.c_str(), MODULE_DOWN_MANUAL, SET);
		}
		else
		{
			setModuleState(target, oam::AUTO_OFFLINE);
		
			//Issue an alarm 
			SNMPManager aManager;				
			aManager.sendAlarmReport(target.c_str(), MODULE_DOWN_AUTO, SET);
		}
	}

	returnStatus = sendMsgProcMon( target, msg, requestID );

	if ( returnStatus == API_SUCCESS)
	{
		//Issue an alarm, log the event 
		log.writeLog(__LINE__, target + " module is successfully stopped.", LOG_TYPE_DEBUG);

		if ( manualFlag ) {
			setModuleState(target, oam::MAN_OFFLINE);
		
			//Issue an alarm 
			SNMPManager aManager;				
			aManager.sendAlarmReport(target.c_str(), MODULE_DOWN_MANUAL, SET);
		}
		else
		{
			setModuleState(target, oam::AUTO_OFFLINE);
		
			//Issue an alarm 
			SNMPManager aManager;
			aManager.sendAlarmReport(target.c_str(), MODULE_DOWN_AUTO, SET);
		}
	}
	else
	{
		if ( manualFlag ) {
			setModuleState(target, oam::FAILED);
		}

		//log the event 
		log.writeLog(__LINE__, target + " module failed to stop!!", LOG_TYPE_WARNING);
	}

	return returnStatus;
}

/******************************************************************************************
* @brief	shutdownModule
*
* purpose:	power off the specified module,
*
******************************************************************************************/
int ProcessManager::shutdownModule(string target, ByteStream::byte actionIndicator, bool manualFlag)
{
	ByteStream msg;
	ByteStream::byte requestID = SHUTDOWNMODULE;
	string processName = "";

	msg = buildRequestMessage(requestID, actionIndicator, processName, manualFlag);

	int returnStatus = sendMsgProcMon( target, msg, requestID );

	if ( returnStatus == API_SUCCESS)
	{
		//Issue an alarm, log the event 
		log.writeLog(__LINE__, target + " module is shutdown by request.", LOG_TYPE_DEBUG);

		if ( manualFlag ) {
			setModuleState(target, oam::MAN_OFFLINE);
		
			//mark all processes running on module man-offline
			setProcessStates(target, oam::MAN_OFFLINE);

			//Issue an alarm 
			SNMPManager aManager;				
			aManager.sendAlarmReport(target.c_str(), MODULE_DOWN_MANUAL, SET);
		}
		else
		{
			setModuleState(target, oam::AUTO_OFFLINE);

			//mark all processes running on module auto-offline
			setProcessStates(target, oam::AUTO_OFFLINE);

			//Issue an alarm 
			SNMPManager aManager;				
			aManager.sendAlarmReport(target.c_str(), MODULE_DOWN_AUTO, SET);
		}
	}
	else
	{
		setModuleState(target, oam::FAILED);
	
		//log the event 
		log.writeLog(__LINE__, target + " module failed to shutdown!!", LOG_TYPE_WARNING);
	}

	return returnStatus;
}

/******************************************************************************************
* @brief	disableModule
*
* purpose:	Set the Disable State on a specified module
*
******************************************************************************************/
int ProcessManager::disableModule(string target, bool manualFlag)
{
	Oam oam;
	ModuleConfig moduleconfig;

	log.writeLog(__LINE__, "disableModule request for " + target, LOG_TYPE_DEBUG);

	string moduleType = target.substr(0,MAX_MODULE_TYPE_SIZE);

	// get current Module Type Count and validate request
	if ( moduleType == "xm" ) {
		log.writeLog(__LINE__, "disableModule - ERROR: can't disable External module type", LOG_TYPE_ERROR);
		return API_INVALID_PARAMETER;
	}

	pthread_mutex_lock(&THREAD_LOCK);

	int newState;
	string SnewState;
	if ( manualFlag ) {
		newState = oam::MAN_DISABLED;
		SnewState = oam::MANDISABLEDSTATE;
	}
	else
	{
		newState = oam::AUTO_DISABLED;
		SnewState = oam::AUTODISABLEDSTATE;
	}

	// skip of module already in current DISABLED state or in MAN_DISABLED state
	try{
		int opState;
		bool degraded;
		oam.getModuleStatus(target, opState, degraded);

		if (opState == newState || opState == oam::MAN_DISABLED) {
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_SUCCESS;
		}

		// if current state is AUTO_DISABLED and new state is MAN_DISABLED
		// update state to MAN_DISABLED

		if (opState == oam::AUTO_DISABLED && newState == oam::MAN_DISABLED) {
		
			try
			{
				oam.getSystemConfig(target, moduleconfig);
		
				moduleconfig.DisableState = oam::MANDISABLEDSTATE;
		
				try
				{
					oam.setSystemConfig(target, moduleconfig);
				}
				catch (exception& ex)
				{
					string error = ex.what();
					log.writeLog(__LINE__, "EXCEPTION ERROR on setSystemConfig: " + error, LOG_TYPE_ERROR);
				}
				catch(...)
				{
					log.writeLog(__LINE__, "EXCEPTION ERROR on setSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
				}
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

			pthread_mutex_unlock(&THREAD_LOCK);
		
			setModuleState(target, oam::MAN_DISABLED);

			return API_SUCCESS;
		}

	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + target + ": " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + target + ": Caught unknown exception!", LOG_TYPE_ERROR);
	}

	pthread_mutex_unlock(&THREAD_LOCK);
	
	if (setEnableState( target, SnewState) != API_SUCCESS )
		return API_FAILURE;

	setModuleState(target, newState);

	log.writeLog(__LINE__, "disableModule - setEnableState", LOG_TYPE_DEBUG);


	//update PMS area if PM was disabled
	if ( moduleType == "pm" ) {
		if ( updatePMSconfig() != API_SUCCESS )
			return API_FAILURE;

		log.writeLog(__LINE__, "disableModule - Updated PM server Count", LOG_TYPE_DEBUG);
	}

	//Update DBRM section of Calpont.xml
	if ( updateWorkerNodeconfig() != API_SUCCESS )
		return API_FAILURE;

	//distribute config file
	distributeConfigFile("system");

	// hallt the dbrm
	log.writeLog(__LINE__, "'dbrmctl halt/reload/resume' starting", LOG_TYPE_DEBUG);
	oam.dbrmctl("halt");
	log.writeLog(__LINE__, "'dbrmctl halt' done", LOG_TYPE_DEBUG);

	//call dbrm control
	oam.dbrmctl("reload");
	log.writeLog(__LINE__, "'dbrmctl reload' done", LOG_TYPE_DEBUG);

	// resume the dbrm
	oam.dbrmctl("resume");
	log.writeLog(__LINE__, "'dbrmctl resume' done", LOG_TYPE_DEBUG);

	log.writeLog(__LINE__, "restarting ExeMgr", LOG_TYPE_DEBUG);
	restartProcessType("ExeMgr");
	log.writeLog(__LINE__, "... ExeMgr, now mysqld", LOG_TYPE_DEBUG);
	restartProcessType("mysql");
	log.writeLog(__LINE__, "done", LOG_TYPE_DEBUG);

	log.writeLog(__LINE__, "disableModule sucessfully complete for " + target, LOG_TYPE_DEBUG);

	return API_SUCCESS;
}

/******************************************************************************************
* @brief	enableModule
*
* purpose:	Clear the Disable State on a specified module
*
******************************************************************************************/
int ProcessManager::enableModule(string target, int state)
{
	Oam oam;
	ModuleConfig moduleconfig;

	log.writeLog(__LINE__, "enableModule request for " + target, LOG_TYPE_DEBUG);

	string moduleType = target.substr(0,MAX_MODULE_TYPE_SIZE);

	// get current Module Type Count and validate request
	if ( moduleType == "xm" ) {
		log.writeLog(__LINE__, "enableModule - ERROR: can't disable External module type", LOG_TYPE_ERROR);
		return API_INVALID_PARAMETER;
	}

	if (setEnableState( target, oam::ENABLEDSTATE) != API_SUCCESS )
		return API_FAILURE;

	setModuleState(target, state);

	//update PMS area if PM was disabled
	if ( moduleType == "pm" ) {
		if ( updatePMSconfig() != API_SUCCESS )
			return API_FAILURE;

		log.writeLog(__LINE__, "enableModule - Updated PM server Count", LOG_TYPE_DEBUG);

	}

	//Update DBRM section of Calpont.xml
	if ( updateWorkerNodeconfig() != API_SUCCESS )
		return API_FAILURE;

	//distribute config file
	distributeConfigFile("system");	

	log.writeLog(__LINE__, "enableModule request for " + target + " completed", LOG_TYPE_DEBUG);			

	return API_SUCCESS;
}


/******************************************************************************************
* @brief	startMgrProcesses
*
* purpose:	start all Mgr Controlled processes for a module
*
******************************************************************************************/
void ProcessManager::startMgrProcesses(std::string moduleName)
{
	Oam oam;
	SystemProcessConfig systemprocessconfig;
	vector<ProcessConfig>::iterator itor;

	ByteStream msg;
	string modulePortName = moduleName + "_ProcessMonitor";

	try{
		oam.getProcessConfig(systemprocessconfig);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: Caught unknown exception!", LOG_TYPE_ERROR);
	}

	string moduleType = moduleName.substr(0,MAX_MODULE_TYPE_SIZE);

	while(true) 
	{
		bool status = true;
		for (itor=systemprocessconfig.processconfig.begin();
			itor != systemprocessconfig.processconfig.end(); ++itor)
		{
			status = true;
	
			if ((*itor).BootLaunch == MGR_LAUNCH)
			{
				if ((*itor).ModuleType == moduleType
				|| (*itor).ModuleType == "ChildExtOAMModule"
				|| ( (*itor).ModuleType == "ChildOAMModule" && moduleName != "xm" )
				|| ((*itor).ModuleType == "ParentOAMModule" && moduleName == config.OAMParentName()) )
				{
					int state;
					try{
						ProcessStatus procstat;
						oam.getProcessStatus((*itor).ProcessName, moduleName, procstat);
						state = procstat.ProcessOpState;
					}
					catch (exception& ex)
					{
						string error = ex.what();
						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
						continue;
					}
					catch(...)
					{
						log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
						continue;
					}
		
					if ( state == oam::INITIAL ) {
		
						msg = buildRequestMessage(START, FORCEFUL, (*itor).ProcessName);
		
						log.writeLog(__LINE__, "Request Start of Process/Module: " + (*itor).ProcessName + " / " + moduleName, LOG_TYPE_DEBUG);
		
						try{
							MessageQueueClient mqRequest(modulePortName);
							mqRequest.write(msg);
							mqRequest.shutdown();
//							sleep(2);
							status = false;
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
					}
				}
			}
		} //end of for loop

		if (status)
			return;
	} //end of while
}

/******************************************************************************************
* @brief	stopProcess
*
* purpose:	Stop a Process on the specified module
*
******************************************************************************************/
int ProcessManager::stopProcess(string moduleName, string processName, 
	messageqcpp::ByteStream::byte actionIndicator, bool manualFlag)
{
	ByteStream msg;
	ByteStream::byte requestID = STOP;
	
	msg = buildRequestMessage(requestID, actionIndicator, processName, manualFlag);

	int returnStatus = sendMsgProcMon( moduleName, msg, requestID );

	if ( returnStatus == API_SUCCESS)
		//log the event 
		log.writeLog(__LINE__, processName + " process is stopped by request.", LOG_TYPE_DEBUG);
	else
		//log the event 
		log.writeLog(__LINE__, processName + " process failed to stop!!", LOG_TYPE_WARNING);

	return returnStatus;
}

/******************************************************************************************
* @brief	startProcess
*
* purpose:	Start a Process on the specified module
*
******************************************************************************************/
int ProcessManager::startProcess(string moduleName, string processName, 
	messageqcpp::ByteStream::byte actionIndicator)
{
	ByteStream msg;
	ByteStream::byte requestID = START;
	
	msg = buildRequestMessage(requestID, actionIndicator, processName);

	int returnStatus = sendMsgProcMon( moduleName, msg, requestID );

	if ( returnStatus == API_SUCCESS)
		//log the event 
		log.writeLog(__LINE__, moduleName + "/" + processName + " process is started by request.", LOG_TYPE_DEBUG);
	else
		//log the event 
		log.writeLog(__LINE__, moduleName + "/" + processName + " process failed to start!!", LOG_TYPE_WARNING);

	return returnStatus;
}

/******************************************************************************************
* @brief	restartProcess
*
* purpose:	Restart a Process on the specified module
*
******************************************************************************************/
int ProcessManager::restartProcess(string moduleName, string processName, 
	messageqcpp::ByteStream::byte actionIndicator, bool manualFlag)
{
	ByteStream msg;
	ByteStream::byte requestID = RESTART;
	
	msg = buildRequestMessage(requestID, actionIndicator, processName, manualFlag);

	int returnStatus;
	// need retry due to the depend process checks
	for ( int retry = 0 ; retry < 5 ; retry++)
	{
		returnStatus = sendMsgProcMon( moduleName, msg, requestID );
	
		if ( returnStatus == API_SUCCESS)
		{
			log.writeLog(__LINE__, processName + " process is restarted by request.", LOG_TYPE_DEBUG);
			return returnStatus;
		}
		else
			log.writeLog(__LINE__, processName + " process failed to restart, will retry!!", LOG_TYPE_WARNING);
		sleep(2);
	}
	return returnStatus;
}

/******************************************************************************************
* @brief	reinitProcess
*
* purpose:	Reinit a Process on the specified module
*
******************************************************************************************/
int ProcessManager::reinitProcess(string moduleName, string processName)
{
	ByteStream msg;
	ByteStream::byte requestID = PROCREINITPROCESS;
	ByteStream::byte actionIndicator = FORCEFUL;

	msg = buildRequestMessage(requestID, actionIndicator, processName);

	int returnStatus = sendMsgProcMon( moduleName, msg, requestID );

	if ( returnStatus == API_SUCCESS)
		//log the event 
		log.writeLog(__LINE__, processName + " process is reinited by request.", LOG_TYPE_DEBUG);
	else
		//log the event 
		log.writeLog(__LINE__, processName + " process failed to reinit!!", LOG_TYPE_WARNING);

	return returnStatus;
}

/******************************************************************************************
* @brief	setSystemState
*
* purpose:	set System State and process required alarms
*
******************************************************************************************/
void ProcessManager::setSystemState(uint16_t state)
{
	ProcessLog log;
	Oam oam;
	SNMPManager aManager;
	Configuration config;

	log.writeLog(__LINE__, "Set System State = " + oam.itoa(state), LOG_TYPE_DEBUG);

	pthread_mutex_lock(&STATUS_LOCK);
	try{
		oam.setSystemStatus(state);
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
	pthread_mutex_unlock(&STATUS_LOCK);

	// Process Alarms
	string system = "System";
	if( state == oam::ACTIVE ) {
		//clear alarms if set
		if ( oam.checkActiveAlarm(SYSTEM_DOWN_AUTO, config.moduleName(), system) )
			aManager.sendAlarmReport(system.c_str(), SYSTEM_DOWN_AUTO, CLEAR);
		if ( oam.checkActiveAlarm(SYSTEM_DOWN_MANUAL, config.moduleName(), system) )
			aManager.sendAlarmReport(system.c_str(), SYSTEM_DOWN_MANUAL, CLEAR);
	}
	else {
		if( state == oam::MAN_OFFLINE )
			aManager.sendAlarmReport(system.c_str(), SYSTEM_DOWN_MANUAL, SET);
		else
			if ( state == oam::AUTO_OFFLINE )
				aManager.sendAlarmReport(system.c_str(), SYSTEM_DOWN_AUTO, SET);
	}
}

/******************************************************************************************
* @brief	setModuleState
*
* purpose:	set Module State of a specific module
*
******************************************************************************************/
void ProcessManager::setModuleState(string moduleName, uint16_t state)
{
	ProcessLog log;
	Oam oam;
	log.writeLog(__LINE__, "Set Module " + moduleName + " State = " + oam.itoa(state), LOG_TYPE_DEBUG);

	pthread_mutex_lock(&STATUS_LOCK);
	try{
		oam.setModuleStatus(moduleName, state);
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

	pthread_mutex_unlock(&STATUS_LOCK);
}

/******************************************************************************************
* @brief	setExtdeviceState
*
* purpose:	set Switch State of a specific switch
*
******************************************************************************************/
void ProcessManager::setExtdeviceState(string extDeviceName, uint16_t state)
{
	ProcessLog log;
	Oam oam;
	log.writeLog(__LINE__, "Set Ext Device " + extDeviceName + " State = " + oam.itoa(state), LOG_TYPE_DEBUG);

	pthread_mutex_lock(&STATUS_LOCK);
	try{
		oam.setExtDeviceStatus(extDeviceName, state);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on setExtDeviceStatus: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on setExtDeviceStatus: Caught unknown exception!", LOG_TYPE_ERROR);
	}
	pthread_mutex_unlock(&STATUS_LOCK);
}

/******************************************************************************************
* @brief	setNICState
*
* purpose:	set NIC State of a specific storage
*
******************************************************************************************/
void ProcessManager::setNICState(string hostName, uint16_t state)
{
	ProcessLog log;
	Oam oam;
	log.writeLog(__LINE__, "Set NIC " + hostName + " State = " + oam.itoa(state), LOG_TYPE_DEBUG);

	pthread_mutex_lock(&STATUS_LOCK);
	try{
		oam.setNICStatus(hostName, state);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on setNICStatus: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on setNICStatus: Caught unknown exception!", LOG_TYPE_ERROR);
	}
	pthread_mutex_unlock(&STATUS_LOCK);
}


/******************************************************************************************
* @brief	setProcessState
*
* purpose:	set Process State of a specific Process
*
******************************************************************************************/
int ProcessManager::setProcessState(string moduleName, string processName, uint16_t state, uint16_t PID)
{
	ProcessLog log;
	Oam oam;
	log.writeLog(__LINE__, "StatusUpdate of Process " + processName + " State = " + oam.itoa(state), LOG_TYPE_DEBUG);

	try {
    	oam.setProcessStatus(processName, moduleName, state, PID);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on setProcessStatus: " + error, LOG_TYPE_ERROR);
		return oam::API_FAILURE;
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on setProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
		return oam::API_FAILURE;
	}

	return oam::API_SUCCESS;
}

/******************************************************************************************
* @brief	setProcessStates
*
* purpose:	set all processes running on a module to requested state
*
******************************************************************************************/
void ProcessManager::setProcessStates(std::string moduleName, uint16_t state, std::string processNameSkip )
{
	ProcessLog log;
	Oam oam;
	log.writeLog(__LINE__, "Set All NON-MAN_OFFLINE Process for module " + moduleName + " = " + oam.itoa(state), LOG_TYPE_DEBUG);

	SystemProcessConfig systemprocessconfig;
	vector<ProcessConfig>::iterator itor;

	try{
		oam.getProcessConfig(systemprocessconfig);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessConfig: Caught unknown exception!", LOG_TYPE_ERROR);
	}

	string moduleType = moduleName.substr(0,MAX_MODULE_TYPE_SIZE);
	
	for (itor=systemprocessconfig.processconfig.begin();
		itor != systemprocessconfig.processconfig.end(); ++itor)
	{
		if ( (*itor).ModuleType == moduleType
			|| (*itor).ModuleType == "ChildExtOAMModule"
			|| ( (*itor).ModuleType == "ChildOAMModule" && moduleName != "xm" )
			|| ((*itor).ModuleType == "ParentOAMModule") )
		{
			if ( (*itor).ProcessName == processNameSkip )
				continue;

			ProcessStatus processstatus;
			try {
				oam.getProcessStatus((*itor).ProcessName, moduleName, processstatus);
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

			if (processstatus.ProcessOpState != oam::MAN_OFFLINE) {
				setProcessState(moduleName, (*itor).ProcessName, state, 0);

				if ( (*itor).ProcessName == "ExeMgr" || state == oam::AUTO_OFFLINE ) 
					setProcessState(moduleName, "mysqld", state, 0);
			}
		}
	}
}

/******************************************************************************************
* @brief	updateLog
*
* purpose:	updatelog on a specific module
*
******************************************************************************************/
int ProcessManager::updateLog(std::string action, std::string moduleName, std::string level)
{
	ByteStream msg;
	ByteStream::byte requestID = PROCUPDATELOG;

	msg << requestID;
	msg << action;
	msg << level;

	int returnStatus = sendMsgProcMon( moduleName, msg, requestID );

	if ( returnStatus == API_SUCCESS)
	{
		//log the success event 
		log.writeLog(__LINE__, moduleName + " updateLog by request.", LOG_TYPE_DEBUG);
	}
	else
	{
		//log the error event 
		log.writeLog(__LINE__, moduleName + " updateLog failed!!", LOG_TYPE_WARNING);
	}
	return returnStatus;
}

/******************************************************************************************
* @brief	getConfigLog
*
* purpose:	get Log Configation on a specific module
*
******************************************************************************************/
int ProcessManager::getConfigLog(std::string moduleName)
{
	ByteStream msg;
	ByteStream::byte requestID = PROCGETCONFIGLOG;

	msg << requestID;

	int returnStatus = sendMsgProcMon( moduleName, msg, requestID );

	return returnStatus;
}

/******************************************************************************************
* @brief	updateConfig
*
* purpose:	Send Msg to Process-Monitor to re-read updated Configation data
*
******************************************************************************************/
int ProcessManager::updateConfig(std::string moduleName)
{
	ByteStream msg;
	ByteStream::byte requestID = PROCUPDATECONFIG;

	msg << requestID;

	int returnStatus = sendMsgProcMon( moduleName, msg, requestID );

	return returnStatus;
}

/******************************************************************************************
* @brief	buildSystemTables
*
* purpose:	Send a Message to 'pm1' to check and build System Table
*
******************************************************************************************/
int ProcessManager::buildSystemTables(string target)
{
	ByteStream msg;
	ByteStream::byte requestID = PROCBUILDSYSTEMTABLES;

	msg << requestID;

	int returnStatus = sendMsgProcMon( target, msg, requestID );

	return returnStatus;
}

/******************************************************************************************
* @brief	stopProcessType
*
* purpose:	Stops a type of process within the system
*
******************************************************************************************/
int ProcessManager::stopProcessType( std::string processName, bool manualFlag )
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	SystemProcessStatus systemprocessstatus;
	ProcessStatus processstatus;

	log.writeLog(__LINE__, "stopProcessType: Stop all " + processName, LOG_TYPE_DEBUG);

	try {
		oam.getProcessStatus(systemprocessstatus);
	
		for( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
		{
			if ( systemprocessstatus.processstatus[i].ProcessName == processName) {
				// found one, request restart of it
				int retStatus = processManager.stopProcess(systemprocessstatus.processstatus[i].Module, 
																processName, 
																GRACEFUL, 
																manualFlag);
				log.writeLog(__LINE__, "stopProcessType: Start ACK received from Process-Monitor, return status = " + oam.itoa(retStatus), LOG_TYPE_DEBUG);
			}
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

	return API_SUCCESS;

}

/******************************************************************************************
* @brief	startProcessType
*
* purpose:	Starts a type of process within the system
*
******************************************************************************************/
int ProcessManager::startProcessType( std::string processName )
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	SystemProcessStatus systemprocessstatus;
	ProcessStatus processstatus;

	log.writeLog(__LINE__, "StartProcessType: Start all " + processName, LOG_TYPE_DEBUG);
	try
	{
		oam.getProcessStatus(systemprocessstatus);

		for( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
		{
			if ( systemprocessstatus.processstatus[i].ProcessName == processName) {
				// found one, request restart of it
				int retStatus = processManager.startProcess(systemprocessstatus.processstatus[i].Module, 
																processName, 
																FORCEFUL);
				log.writeLog(__LINE__, "StartProcessType: Start ACK received from Process-Monitor, return status = " + oam.itoa(retStatus), LOG_TYPE_DEBUG);
			}
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

	return API_SUCCESS;
}

/******************************************************************************************
* @brief	restartProcessType
*
* purpose:	Restarts ACTIVE type of process within the system
*
******************************************************************************************/
int ProcessManager::restartProcessType( std::string processName )
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	SystemProcessStatus systemprocessstatus;
	ProcessStatus processstatus;
	int retStatus = API_SUCCESS;

	log.writeLog(__LINE__, "restartProcessType: Restart all " + processName, LOG_TYPE_DEBUG);

	// If mysql is the processName, then send to modules were ExeMgr is running
	try
	{
		oam.getProcessStatus(systemprocessstatus);

		for( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
		{
			if ( processName == "mysql" ) {
				if ( systemprocessstatus.processstatus[i].ProcessName == "ExeMgr") {
					ProcessStatus procstat;
					oam.getProcessStatus("mysqld", systemprocessstatus.processstatus[i].Module, procstat);
					int state = procstat.ProcessOpState;
					if ( state == ACTIVE ) {
						retStatus = processManager.restartProcess(systemprocessstatus.processstatus[i].Module, 
																		processName, 
																		FORCEFUL, 
																		true);
						log.writeLog(__LINE__, "restartProcessType: Start ACK received from Process-Monitor, return status = " + oam.itoa(retStatus), LOG_TYPE_DEBUG);
					}
				}
			}
			else
			{
				if ( systemprocessstatus.processstatus[i].ProcessName == processName &&
					systemprocessstatus.processstatus[i].ProcessOpState == oam::ACTIVE ) {
					// found one, request restart of it
					retStatus = processManager.restartProcess(systemprocessstatus.processstatus[i].Module, 
																	processName, 
																	FORCEFUL, 
																	true);
					log.writeLog(__LINE__, "restartProcessType: Start ACK received from Process-Monitor, return status = " + oam.itoa(retStatus), LOG_TYPE_DEBUG);
				}
			}
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

	return retStatus;
}

/******************************************************************************************
* @brief	reinitProcessType
*
* purpose:	Reinit ACTIVE type of process within the system
*
******************************************************************************************/
int ProcessManager::reinitProcessType( std::string processName )
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	SystemProcessStatus systemprocessstatus;
	ProcessStatus processstatus;
	int retStatus = API_SUCCESS;

	log.writeLog(__LINE__, "reinitProcessType: ReInit all " + processName, LOG_TYPE_DEBUG);
	try
	{
		oam.getProcessStatus(systemprocessstatus);

		for( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
		{
			if ( systemprocessstatus.processstatus[i].ProcessName == processName &&
				 systemprocessstatus.processstatus[i].ProcessOpState == oam::ACTIVE ) {
				// found one, request reinit of it
				retStatus = processManager.reinitProcess(systemprocessstatus.processstatus[i].Module, 
																processName);
				log.writeLog(__LINE__, "reinitProcessType: ACK received from Process-Monitor, return status = " + oam.itoa(retStatus), LOG_TYPE_DEBUG);
			}
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

	return retStatus;
}

/******************************************************************************************
* @brief	addModule
*
* purpose:	Add Module to system configuration
*
******************************************************************************************/
int ProcessManager::addModule(oam::DeviceNetworkList devicenetworklist, std::string password)
{
	SystemModuleTypeConfig systemmoduletypeconfig;
	ModuleTypeConfig moduletypeconfig;
	ModuleTypeConfig setmoduletypeconfig;
	DeviceNetworkConfig devicenetworkconfig;
	Oam oam;
	string Section;

	pthread_mutex_lock(&THREAD_LOCK);

	int AddModuleCount = devicenetworklist.size();
	DeviceNetworkList::iterator listPT = devicenetworklist.begin();
	string moduleType = (*listPT).DeviceName.substr(0,MAX_MODULE_TYPE_SIZE);

	if ( moduleType == "xm" ) {
		// uncomment Extext Map Section
		if( !updateExtentMap() ) {
			log.writeLog(__LINE__, "addModule - ERROR: Update Calpont.xml Extent Map Section", LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_FAILURE;
		}
	}
	
	//
	//Check hostname and IP Address for availibility
	//
	try
	{
		oam.getSystemConfig(systemmoduletypeconfig);

		for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
		{
			if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
				// end of list
				break;

			int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
			string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;

			if ( moduleCount > 0 )
			{
				DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
				for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
				{
					HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
					for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
					{
						string hostname = (*pt1).HostName;
						string ipAddr = (*pt1).IPAddr;
	
						listPT = devicenetworklist.begin();
						for( ; listPT != devicenetworklist.end() ; listPT++)
						{
							HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
							string newHostName = (*pt1).HostName;
							string newIPAddr = (*pt1).IPAddr;
	
							if ( newIPAddr == ipAddr || newHostName == hostname ) {
								log.writeLog(__LINE__, "addModule - ERROR: hostName or IP address already in-use: " + newIPAddr + "/" + newHostName, LOG_TYPE_ERROR);
								pthread_mutex_unlock(&THREAD_LOCK);
								return API_INVALID_PARAMETER;
							}
						}
					}
				}
			}
		}
	}
	catch (exception& e)
	{
		log.writeLog(__LINE__, "addModule - ERROR: getSystemConfig", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	string calpontPackage;
	string mysqlPackage;
	string mysqldPackage;
	string systemID;
	string packageType = "rpm";

	oam.getSystemConfig("EEPackageType", packageType);

	if ( moduleType != "xm" ) {
		//
		// check for RPM package
		//
	
		SystemSoftware systemsoftware;
	
		try
		{
			oam.getSystemSoftware(systemsoftware);
		}
		catch (exception& e)
		{
			log.writeLog(__LINE__, "addModule - ERROR: getSystemSoftware", LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_FAILURE;
		}
	
		//check if pkgs are located in /root directory
		if ( packageType != "binary") {
			string seperator = "-";
			if ( packageType == "deb" )
				seperator = "_";
			calpontPackage = "/root/calpont" + seperator + systemsoftware.Version + "-" + systemsoftware.Release + "*." + packageType;
			mysqlPackage = "/root/calpont-mysql" + seperator + systemsoftware.Version + "-" + systemsoftware.Release + "*." + packageType;
			mysqldPackage = "/root/calpont-mysqld" + seperator + systemsoftware.Version + "-" + systemsoftware.Release + "*." + packageType;
		}
		else
		{
			calpontPackage = "/root/calpont-infinidb-ent-" + systemsoftware.Version + "-" + systemsoftware.Release + "*.bin.tar.gz";
			mysqlPackage = calpontPackage;
			mysqldPackage = calpontPackage;
		}

		string cmd = "ls " + calpontPackage + " > /dev/null 2>&1";
		int rtnCode = system(cmd.c_str());
		if (rtnCode != 0) {
			log.writeLog(__LINE__, "addModule - ERROR: Package not found: " + calpontPackage, LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_FILE_OPEN_ERROR;
		}
		log.writeLog(__LINE__, "addModule - Calpont Package found:" + calpontPackage, LOG_TYPE_DEBUG);
	
		//
		// Verify Host IP and Password
		//
	
		listPT = devicenetworklist.begin();
		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
			string newIPAddr = (*pt1).IPAddr;
	
			string cmd = "/usr/local/Calpont/bin/remote_command.sh " + newIPAddr + " " + password + " ls";
			log.writeLog(__LINE__, cmd, LOG_TYPE_DEBUG);
			int rtnCode = system(cmd.c_str());
			if (rtnCode != 0) {
				log.writeLog(__LINE__, "addModule - ERROR: Remote login test failed, Invalid IP / Password " + newIPAddr + "/" + password, LOG_TYPE_ERROR);
				pthread_mutex_unlock(&THREAD_LOCK);
				return API_FAILURE;
			}
			log.writeLog(__LINE__, "addModule - Remote login test successful: " + newIPAddr, LOG_TYPE_DEBUG);
		}
	}
	else
	{
		//get system ID from DisableState (cheating)
		listPT = devicenetworklist.begin();
		systemID = (*listPT).DisableState;
	}

	//
	//Get System Configuration file
	//

	listPT = devicenetworklist.begin();

	try{
		oam.getSystemConfig(moduleType, moduletypeconfig);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "addModule - ERROR: getSystemConfig", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}
	setmoduletypeconfig = moduletypeconfig;

	// update Module Type Count
	int oldModuleCount = moduletypeconfig.ModuleCount;
	int newModuleCount = oldModuleCount + AddModuleCount;
	setmoduletypeconfig.ModuleCount = newModuleCount;

	//add new IP Addresses and Hostnames
	listPT = devicenetworklist.begin();
	HostConfig hostconfig;
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		devicenetworkconfig.DeviceName = (*listPT).DeviceName;

		HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
		for( ; pt1 != (*listPT).hostConfigList.end() ; pt1++)
		{
			hostconfig.IPAddr = (*pt1).IPAddr;
			hostconfig.HostName = (*pt1).HostName;
			hostconfig.NicID = (*pt1).NicID;
			devicenetworkconfig.hostConfigList.push_back(hostconfig);
		}
		setmoduletypeconfig.ModuleNetworkList.push_back(devicenetworkconfig);
	}

	Config* sysConfig = Config::makeConfig();

	//Add additional Process Ports
	// all nodes: ProcessMonitor, ServerMonitor
	// dm: NONE
	// um: ExeMgr
	// pm: NONE

	listPT = devicenetworklist.begin();
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		Section = (*listPT).DeviceName + "_ProcessMonitor";

		HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
		sysConfig->setConfig(Section, "IPAddr", (*pt1).IPAddr);
		if ( moduleType == "xm" ) {
			sysConfig->setConfig(Section, "Port", "88" + systemID );
		}
		else
		{
			sysConfig->setConfig(Section, "Port", "8800");

			Section = (*listPT).DeviceName + "_ServerMonitor";
			sysConfig->setConfig(Section, "IPAddr", (*pt1).IPAddr);
			sysConfig->setConfig(Section, "Port", "8622");
		}
	}

	if ( moduleType == "um" ||
		( moduleType == "pm" && config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM ) ) {
		listPT = devicenetworklist.begin();
		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			int moduleID = atoi((*listPT).DeviceName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());

			Section = "ExeMgr" + oam.itoa(moduleID);
			HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
			sysConfig->setConfig(Section, "IPAddr", (*pt1).IPAddr);
			sysConfig->setConfig(Section, "Port", "8601");
		}
	}

	log.writeLog(__LINE__, "addModule - Updated Process Ports", LOG_TYPE_DEBUG);

	if ( moduleType == "xm" ) {
		//update Extext Map BRM_UID
		string BRM_UID = "0x" + systemID;
	
		try {
			sysConfig->setConfig("ExtentMap", "BRM_UID", BRM_UID);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "addModule - ERROR: Problem setting BRM_UID in the Calpont System Configuration file", LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_FAILURE;
		}
	}

	string parentOAMModuleHostName;
	string parentOAMModuleIPAddr;
	if ( moduleType == "xm" ) {
		ModuleConfig moduleconfig;
		string parentOAMModuleName;

		try{
			parentOAMModuleName = sysConfig->getConfig("SystemConfig", "ParentOAMModuleName");
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on ParentOAMModuleName: Caught unknown exception!", LOG_TYPE_ERROR);
		}

		try {
			oam.getSystemConfig(parentOAMModuleName, moduleconfig);
			HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
			parentOAMModuleHostName = (*pt1).HostName;
			parentOAMModuleIPAddr = (*pt1).IPAddr;
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
	}

	//update Calpont Config table
	try {
		sysConfig->write();
	}
	catch(...)
	{
		log.writeLog(__LINE__, "addModule - ERROR: sysConfig->write", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	//write Calpont.xml Module section
	try {
		oam.setSystemConfig(moduleType, setmoduletypeconfig);
		log.writeLog(__LINE__, "addModule - Updated Module Section of Config file", LOG_TYPE_DEBUG);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "addModule - ERROR: setSystemConfig", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	pthread_mutex_unlock(&THREAD_LOCK);

	//
	//send message to Process Monitor to add new modules/processes/nics to shared memory
	//
	try
	{
		ByteStream obs;

		obs << (ByteStream::byte) ADD_MODULE;
        obs << (ByteStream::byte) AddModuleCount;

		listPT = devicenetworklist.begin();
		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			obs << (*listPT).DeviceName;
		}

		//pass NIC Hostnames
		vector<string> nicHostNames;

		listPT = devicenetworklist.begin();
		HostConfig hostconfig;
		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
			for( ; pt1 != (*listPT).hostConfigList.end() ; pt1++)
			{
				nicHostNames.push_back((*pt1).HostName);
			}
		}

		obs << (ByteStream::byte) nicHostNames.size();

		vector<string>::iterator pt2 = nicHostNames.begin();
		for( ; pt2 != nicHostNames.end() ; pt2++)
		{
			obs << *pt2;
		}

		sendStatusUpdate(obs, ADD_MODULE);
		log.writeLog(__LINE__, "addModule - Updated Shared Memory", LOG_TYPE_DEBUG);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "addModule - ERROR: sendStatusUpdate error", LOG_TYPE_ERROR);
		return API_FAILURE;
	}

	//setup and push custom OS files
	listPT = devicenetworklist.begin();
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		string remoteModuleName = (*listPT).DeviceName;
		string remoteModuleType = remoteModuleName.substr(0,MAX_MODULE_TYPE_SIZE);
		HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
		string remoteModuleIP = (*pt1).IPAddr;
		string remoteHostName = (*pt1).HostName;

		//create and copy custom OS into /usr/local/Calpont/local/etc/"modulename"
		//run remote installer script
		string dir = "/usr/local/Calpont/local/etc/" + remoteModuleName;

		string cmd = "mkdir " + dir + " > /dev/null 2>&1";
		int rtnCode = system(cmd.c_str());

		if ( remoteModuleType == "um" ) {
			ifstream file ("/usr/local/Calpont/local/etc/um1/fstab.calpont");
			if (!file) {
				log.writeLog(__LINE__, "addModule - FAILED: missing um1 fstab file", LOG_TYPE_ERROR);
				pthread_mutex_unlock(&THREAD_LOCK);
				return API_FAILURE;
			}

			cmd = "cp /usr/local/Calpont/local/etc/um1/* " + dir + "/.";
			rtnCode = system(cmd.c_str());
			if (rtnCode != 0)
				log.writeLog(__LINE__, "addModule - ERROR: cp to " + dir + " failed", LOG_TYPE_ERROR);
		}
		else
		{
			if ( remoteModuleType == "pm") {
				ifstream file ("/usr/local/Calpont/local/etc/pm1/fstab.calpont");
				if (!file) {
					log.writeLog(__LINE__, "addModule - FAILED: missing pm1 fstab file", LOG_TYPE_ERROR);
					pthread_mutex_unlock(&THREAD_LOCK);
					return API_FAILURE;
				}

				cmd = "cp /usr/local/Calpont/local/etc/pm1/* " + dir + "/.";
				rtnCode = system(cmd.c_str());
				if (rtnCode != 0)
					log.writeLog(__LINE__, "addModule - ERROR: cp to " + dir + " failed", LOG_TYPE_ERROR);
			}
			else
			{	//xm setup
				if( !makeXMInittab(remoteModuleName, systemID, parentOAMModuleHostName) )
					log.writeLog(__LINE__, "addModule - ERROR: makeXMInittab", LOG_TYPE_ERROR);	

				if( !setXMmount(remoteModuleName, parentOAMModuleHostName, parentOAMModuleIPAddr) )
					log.writeLog(__LINE__, "addModule - ERROR: setXMmount", LOG_TYPE_ERROR);
			}
		}
		log.writeLog(__LINE__, "addModule - created directory and custom OS files for " +  remoteModuleName, LOG_TYPE_DEBUG);

		//create module file in /usr/local/Calpont/local/etc/"modulename"
		if ( remoteModuleType != "xm" ) {
			if( !createModuleFile(remoteModuleName) ) {
				log.writeLog(__LINE__, "addModule - ERROR: createModuleFile failed", LOG_TYPE_ERROR);
				pthread_mutex_unlock(&THREAD_LOCK);
				return API_FAILURE;
			}
			log.writeLog(__LINE__, "addModule - create module file for " +  remoteModuleName, LOG_TYPE_DEBUG);
		}

		//add new module info to pm exports file
		if( !updateExports(remoteModuleIP) ) {
			log.writeLog(__LINE__, "addModule - ERROR: updateExports failed", LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_FAILURE;
		}
		log.writeLog(__LINE__, "addModule - updated local export file for " +  remoteModuleName, LOG_TYPE_DEBUG);

		if ( remoteModuleType == "pm" ) {
			//setup Standby OAM Parent, if needed
			if ( config.OAMStandbyName() == oam::UnassignedName )
				setStandbyModule(remoteModuleName, false);
		}

		//run installer on remote module
		if ( remoteModuleType == "um" ||
			( remoteModuleType == "pm" && config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM ) ) {
			//run remote installer script
			if ( packageType != "binary" ) {
				log.writeLog(__LINE__, "addModule - user_installer run for " +  remoteModuleName, LOG_TYPE_DEBUG);
				string cmd = "/usr/local/Calpont/bin/user_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage + " " + mysqlPackage + " " + mysqldPackage + " initial " + packageType + " --nodeps 0";
				rtnCode = system(cmd.c_str());
				if (rtnCode != 0) {
					log.writeLog(__LINE__, "addModule - ERROR: user_installer.sh failed", LOG_TYPE_ERROR);
					pthread_mutex_unlock(&THREAD_LOCK);
					return API_FAILURE;
				}
			}
			else
			{	// do a binary package install
				log.writeLog(__LINE__, "addModule - user_installer run for " +  remoteModuleName, LOG_TYPE_DEBUG);
				string cmd = "/usr/local/Calpont/bin/binary_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage + " " + remoteModuleType + " initial " + packageType + " 0";
				rtnCode = system(cmd.c_str());
				if (rtnCode != 0) {
					log.writeLog(__LINE__, "addModule - ERROR: binary_installer.sh failed", LOG_TYPE_ERROR);
					pthread_mutex_unlock(&THREAD_LOCK);
					return API_FAILURE;
				}
			}
		}
		else
		{
			if ( remoteModuleType == "pm" ) {
				if ( packageType != "binary" ) {
					log.writeLog(__LINE__, "addModule - performance_installer run for " +  remoteModuleName, LOG_TYPE_DEBUG);
					string cmd = "/usr/local/Calpont/bin/performance_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage + " " + mysqlPackage + " " + mysqldPackage + " initial " + packageType + " --nodeps 0";
					rtnCode = system(cmd.c_str());
					if (rtnCode != 0) {
						log.writeLog(__LINE__, "addModule - ERROR: performance_installer.sh failed", LOG_TYPE_ERROR);
						pthread_mutex_unlock(&THREAD_LOCK);
						return API_FAILURE;
					}

				}
				else
				{	// do a binary package install
					log.writeLog(__LINE__, "addModule - performance_installer run for " +  remoteModuleName, LOG_TYPE_DEBUG);
					string cmd = "/usr/local/Calpont/bin/binary_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage + " " + remoteModuleType + + " initial " + packageType + " 0";
					rtnCode = system(cmd.c_str());
					if (rtnCode != 0) {
						log.writeLog(__LINE__, "addModule - ERROR: binary_installer.sh failed", LOG_TYPE_ERROR);
						pthread_mutex_unlock(&THREAD_LOCK);
						return API_FAILURE;
					}
				}
			}
		}
	}

	if ( moduleType != "xm" ) {
		//Start new modules by starting up local Process-Monitor
		listPT = devicenetworklist.begin();
		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			string remoteModuleName = (*listPT).DeviceName;

			//set down Active module to disable state
			disableModule(remoteModuleName, true);

			HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
			string remoteModuleIP = (*pt1).IPAddr;
	
			//send start service commands
			string cmd = "/usr/local/Calpont/bin/remote_command.sh " + remoteModuleIP + " " + password + " '/etc/init.d/infinidb restart;/etc/init.d/mysqld-Calpont restart' 0";
			system(cmd.c_str());
			log.writeLog(__LINE__, "addModule - restart infinidb service " +  remoteModuleName, LOG_TYPE_DEBUG);
		}
	}

	//distribute config file
	distributeConfigFile("system");	

	return API_SUCCESS;
}

/******************************************************************************************
* @brief	removeModule
*
* purpose:	Remove Module to system configuration
*
******************************************************************************************/
int ProcessManager::removeModule(oam::DeviceNetworkList devicenetworklist)
{
	ModuleTypeConfig moduletypeconfig;
	ModuleTypeConfig setmoduletypeconfig;
	Oam oam;
	string Section;

	pthread_mutex_lock(&THREAD_LOCK);

	//get module count being removed
	int RemoveModuleCount = devicenetworklist.size();
	DeviceNetworkList::iterator listPT = devicenetworklist.begin();

	//
	//Get System Configuration 
	//
	listPT = devicenetworklist.begin();
	string moduleType = (*listPT).DeviceName.substr(0,MAX_MODULE_TYPE_SIZE);

	try{
		oam.getSystemConfig(moduleType, moduletypeconfig);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "removeModule - ERROR: getSystemConfig", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}
	setmoduletypeconfig = moduletypeconfig;

	// get current Module Type Count and validate request
	int oldModuleCount = moduletypeconfig.ModuleCount;

	if ( oldModuleCount < RemoveModuleCount ) {
		log.writeLog(__LINE__, "removeModule - ERROR: remove count is larger than ModuleType count", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_INVALID_PARAMETER;
	}

	//stopModules being removed with the REMOVE option, which will stop process
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		string moduleName = (*listPT).DeviceName;
		int status;
		status = stopModule(moduleName, REMOVE, true);

		if (status == API_SUCCESS) {
			log.writeLog(__LINE__, "removeModule - stopModule successfull " + moduleName, LOG_TYPE_DEBUG);
			//check for SIMPLEX Processes on mate might need to be started
			pthread_mutex_unlock(&THREAD_LOCK);
			checkSimplexModule(moduleName);
			pthread_mutex_lock(&THREAD_LOCK);
		}
		else
			log.writeLog(__LINE__, "removeModule - stopModule " + moduleName, LOG_TYPE_ERROR);
	}

	int newModuleCount = oldModuleCount - RemoveModuleCount;
	setmoduletypeconfig.ModuleCount = newModuleCount;

	//Clear out Module IP and Hostnames
	listPT = devicenetworklist.begin();
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		string moduleName = (*listPT).DeviceName;

		DeviceNetworkList::iterator pt = setmoduletypeconfig.ModuleNetworkList.begin();
		for ( ; pt != setmoduletypeconfig.ModuleNetworkList.end() ; pt++)
		{
			if ( moduleName == (*pt).DeviceName ) {
				HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
				for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++ )
				{
					clearNICAlarms((*pt1).HostName);
					(*pt1).IPAddr = oam::UnassignedIpAddr;
					(*pt1).HostName = oam::UnassignedName;
				}
				break;
			}
		}
	}

	//Remove Process Ports
	// all nodes: ProcessMonitor, ServerMonitor
	// dm: NONE
	// um: ExeMgr
	// pm: NONE

	Config* sysConfig = Config::makeConfig();

	listPT = devicenetworklist.begin();
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		Section = (*listPT).DeviceName + "_ProcessMonitor";
		sysConfig->setConfig(Section, "IPAddr", oam::UnassignedIpAddr);

		Section = (*listPT).DeviceName + "_ServerMonitor";
		sysConfig->setConfig(Section, "IPAddr", oam::UnassignedIpAddr);
	}

	if ( moduleType == "um" ||
		( moduleType == "pm" && config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM ) ||
		( moduleType == "um" && config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM ) ||
		( moduleType == "pm" && config.ServerInstallType() == oam::INSTALL_COMBINE_PM_UM ) ) {

		listPT = devicenetworklist.begin();
		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			int moduleID = atoi((*listPT).DeviceName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());

			Section = "ExeMgr" + oam.itoa(moduleID);
			sysConfig->setConfig(Section, "IPAddr", oam::UnassignedIpAddr);
		}
	}

	log.writeLog(__LINE__, "removeModule - Updated Process Ports", LOG_TYPE_DEBUG);

	//remove associated User Temp Storage Names
	if ( moduleType == "um" ||
		( moduleType == "pm" && config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM ) ||
		( moduleType == "um" && config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM ) ||
		( moduleType == "pm" && config.ServerInstallType() == oam::INSTALL_COMBINE_PM_UM ) ) {

		listPT = devicenetworklist.begin();
		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			int moduleID = atoi((*listPT).DeviceName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
			string USERSTORAGELOC = "UserStorageLoc" + oam.itoa(moduleID);
		
			try {
				sysConfig->setConfig("Installation", USERSTORAGELOC, "unassigned");
				log.writeLog(__LINE__, "Successful updated " + USERSTORAGELOC + " as 'unassigned'", LOG_TYPE_DEBUG);
			}
			catch(...)
			{
				log.writeLog(__LINE__, "removeModule - ERROR: setting UserTempStorageName", LOG_TYPE_ERROR);
				pthread_mutex_unlock(&THREAD_LOCK);
				return API_FAILURE;
			}
		}
	}

	//update Calpont Config table
	try {
		sysConfig->write();
	}
	catch(...)
	{
		log.writeLog(__LINE__, "removeModule - ERROR: sysConfig->write", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	//write Calpont.xml Module section
	try {
		oam.setSystemConfig(moduleType, setmoduletypeconfig);
		log.writeLog(__LINE__, "removeModule - Updated Module Section of Config file", LOG_TYPE_DEBUG);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "removeModule - ERROR: setSystemConfig", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	pthread_mutex_unlock(&THREAD_LOCK);

	//check if any removed modules was Standby OAM
	listPT = devicenetworklist.begin();
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		if ( (*listPT).DeviceName == config.OAMStandbyName() ) {
			clearStandbyModule();
			break;
		}
	}

	//
	//send message to Process Monitor to remove module/processes to shared memory
	//
	try
	{
		ByteStream obs;

		obs << (ByteStream::byte) REMOVE_MODULE;
        obs << (ByteStream::byte) RemoveModuleCount;

		listPT = devicenetworklist.begin();
		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			obs << (*listPT).DeviceName;
		}

		sendStatusUpdate(obs, REMOVE_MODULE);
		log.writeLog(__LINE__, "removeModule - Updated Shared Memory", LOG_TYPE_DEBUG);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "removeModule - ERROR: sendStatusUpdate error", LOG_TYPE_ERROR);
		return API_FAILURE;
	}

	if ( moduleType == "pm" ) {
		if ( updatePMSconfig() != API_SUCCESS )
			return API_FAILURE;
	}

	//Update DBRM section of Calpont.xml
	if ( updateWorkerNodeconfig() != API_SUCCESS )
		return API_FAILURE;

	// remove all associated alarms for this modules being removed
	listPT = devicenetworklist.begin();
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		clearModuleAlarms( (*listPT).DeviceName );
	}

	//distribute config file
	distributeConfigFile("system");	
	
	return API_SUCCESS;
}

/******************************************************************************************
* @brief	reconfigureModule
*
* purpose:	Reconfigure Module in system configuration
*
******************************************************************************************/
int ProcessManager::reconfigureModule(oam::DeviceNetworkList devicenetworklist)
{
	ModuleTypeConfig reconfiguremoduletypeconfig;
	ModuleTypeConfig setreconfiguremoduletypeconfig;
	ModuleTypeConfig moduletypeconfig;
	DeviceNetworkConfig devicenetworkconfig;
	Oam oam;
	string Section;

	pthread_mutex_lock(&THREAD_LOCK);

	DeviceNetworkList::iterator listPT = devicenetworklist.begin();

	//get module name being reconfigured
	string moduleName = (*listPT).DeviceName;
	string moduleType = moduleName.substr(0,MAX_MODULE_TYPE_SIZE);

	//get module type being configured as
	listPT++;
	string reconfigureModuleName = (*listPT).DeviceName;
	string reconfigureModuleType = reconfigureModuleName.substr(0,MAX_MODULE_TYPE_SIZE);
	string reconfigureHostName2;
	string reconfigureIpAddr2;
	int reconfigureNicId2 = 0;

	if ( !(*listPT).hostConfigList.empty()) {
		HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
		reconfigureHostName2 = (*pt1).HostName;
		reconfigureIpAddr2 = (*pt1).IPAddr;
		reconfigureNicId2 = (*pt1).NicID;
	}

	int status = stopModule(moduleName, GRACEFUL, true);
	if (status == API_SUCCESS) {
		log.writeLog(__LINE__, "reconfigureModule - stopModule successfull " + moduleName, LOG_TYPE_DEBUG);
		//check for SIMPLEX Processes on mate might need to be started
		pthread_mutex_unlock(&THREAD_LOCK);
		checkSimplexModule(moduleName);
		pthread_mutex_lock(&THREAD_LOCK);
	}
	else
		log.writeLog(__LINE__, "reconfigureModule - stopModule " + moduleName, LOG_TYPE_ERROR);

	//
	//Get Module Configuration 
	//

	try{
		oam.getSystemConfig(moduleType, moduletypeconfig);
		oam.getSystemConfig(reconfigureModuleType, reconfiguremoduletypeconfig);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "reconfigureModule - ERROR: getSystemConfig", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	setreconfiguremoduletypeconfig = reconfiguremoduletypeconfig;

	// update Module Type Counts
	setreconfiguremoduletypeconfig.ModuleCount++;

	Config* sysConfig = Config::makeConfig();

	//Move Module IP and Hostnames
	string IPaddress = oam::UnassignedIpAddr;
	HostConfig hostconfig;
	DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();
	for ( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
	{
		if ( moduleName == (*pt).DeviceName ) {
			devicenetworkconfig.DeviceName = reconfigureModuleName;
			HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
			for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
			{
				if ( pt1 == (*pt).hostConfigList.begin() )
					//save first IP for Process Port usage
					IPaddress = (*pt1).IPAddr;
				hostconfig.IPAddr = (*pt1).IPAddr;
				hostconfig.HostName = (*pt1).HostName;
				hostconfig.NicID = (*pt1).NicID;
				devicenetworkconfig.hostConfigList.push_back(hostconfig);
			}

			//configure any secondary NIC info passed from console
			if ( ! reconfigureHostName2.empty() ) {
				hostconfig.IPAddr = reconfigureIpAddr2;
				hostconfig.HostName = reconfigureHostName2;
				hostconfig.NicID = reconfigureNicId2;
				devicenetworkconfig.hostConfigList.push_back(hostconfig);
			}

			setreconfiguremoduletypeconfig.ModuleNetworkList.push_back(devicenetworkconfig);
			break;
		}
	}

	if ( IPaddress == oam::UnassignedIpAddr ) {
		log.writeLog(__LINE__, "reconfigureModule - ERROR: module IP is unassigned", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	//Update Process Ports
	// all nodes: ProcessMonitor, ServerMonitor
	// dm: NONE
	// um: ExeMgr
	// pm: NONE

	Section = reconfigureModuleName + "_ProcessMonitor";
	sysConfig->setConfig(Section, "IPAddr", IPaddress);
	sysConfig->setConfig(Section, "Port", "8800");

	Section = reconfigureModuleName + "_ServerMonitor";
	sysConfig->setConfig(Section, "IPAddr", IPaddress);
	sysConfig->setConfig(Section, "Port", "8622");

	if ( moduleType == "um" ||
		( moduleType == "pm" && config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM ) ||
		( moduleType == "pm" && config.ServerInstallType() == oam::INSTALL_COMBINE_PM_UM ) ) {

		int moduleID = atoi(moduleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
		Section = "ExeMgr" + oam.itoa(moduleID);
		sysConfig->setConfig(Section, "IPAddr", oam::UnassignedIpAddr);
	}
	else
	{
		//PM TO UM
		int moduleID = atoi(reconfigureModuleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
		Section = "ExeMgr" + oam.itoa(moduleID);
		sysConfig->setConfig(Section, "IPAddr", IPaddress);
		sysConfig->setConfig(Section, "Port", "8601");
	}

	log.writeLog(__LINE__, "reconfigureModule - Updated Process Ports", LOG_TYPE_DEBUG);

	//update Calpont Config table
	try {
		sysConfig->write();
	}
	catch(...)
	{
		log.writeLog(__LINE__, "reconfigureModule - ERROR: sysConfig->write", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	//write Calpont.xml Module section
	try {
		oam.setSystemConfig(reconfigureModuleType, setreconfiguremoduletypeconfig);
		log.writeLog(__LINE__, "reconfigureModule - Updated Module Section of Config file", LOG_TYPE_DEBUG);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "reconfigureModule - ERROR: setSystemConfig", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	//distribute config file
	distributeConfigFile(moduleName);	

	//
	//Send Reconfigure msg to Module's Process-Monitor being reconfigured
	//
	ByteStream msg;
	ByteStream::byte requestID = RECONFIGURE;

	msg << requestID;
	msg << reconfigureModuleName;

	int returnStatus = sendMsgProcMon( moduleName, msg, requestID );

	if ( returnStatus == API_SUCCESS)
		//log the event 
		log.writeLog(__LINE__, "reconfigureModule - procmon reconfigure successful", LOG_TYPE_DEBUG);
	else
	{
		log.writeLog(__LINE__, "reconfigureModule - procmon reconfigure failed", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	ModuleTypeConfig setmoduletypeconfig;

	try{
		oam.getSystemConfig(moduleType, setmoduletypeconfig);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "reconfigureModule - ERROR: getSystemConfig", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	// update Module Type Counts
	setmoduletypeconfig.ModuleCount--;

	//Clear Module IP and Hostnames
	pt = setmoduletypeconfig.ModuleNetworkList.begin();
	for ( ; pt != setmoduletypeconfig.ModuleNetworkList.end() ; pt++)
	{
		if ( moduleName == (*pt).DeviceName ) {
			HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
			for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
			{
				(*pt1).IPAddr = oam::UnassignedIpAddr;
				(*pt1).HostName = oam::UnassignedName;
			}

			break;
		}
	}

	//Update Process Ports
	// all nodes: ProcessMonitor, ServerMonitor
	// dm: NONE
	// um: ExeMgr
	// pm: NONE

	Section = moduleName + "_ProcessMonitor";
	sysConfig->setConfig(Section, "IPAddr", oam::UnassignedIpAddr);

	Section = moduleName + "_ServerMonitor";
	sysConfig->setConfig(Section, "IPAddr", oam::UnassignedIpAddr);

	log.writeLog(__LINE__, "reconfigureModule - Updated Process Ports", LOG_TYPE_DEBUG);

	//update Calpont Config table
	try {
		sysConfig->write();
	}
	catch(...)
	{
		log.writeLog(__LINE__, "reconfigureModule - ERROR: sysConfig->write", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	//write Calpont.xml Module section
	try {
		oam.setSystemConfig(moduleType, setmoduletypeconfig);
		log.writeLog(__LINE__, "reconfigureModule - Updated Module Section of Config file", LOG_TYPE_DEBUG);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "reconfigureModule - ERROR: setSystemConfig", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	pthread_mutex_unlock(&THREAD_LOCK);

	//
	//send message to Process Monitor to remove/add module/processes to shared memory
	//
	try
	{
		ByteStream obs;

		obs << (ByteStream::byte) REMOVE_MODULE;

		obs << (ByteStream::byte) 1;
		obs << moduleName;

		sendStatusUpdate(obs, REMOVE_MODULE);
		log.writeLog(__LINE__, "reconfigureModule - module removed from Shared Memory", LOG_TYPE_DEBUG);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "reconfigureModule - ERROR: sendStatusUpdate error", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FAILURE;
	}

	try
	{
		ByteStream obs;

		obs << (ByteStream::byte) ADD_MODULE;

		obs << (ByteStream::byte) 1;
		obs << reconfigureModuleName;

		//pass NIC Hostnames
		if ( ! reconfigureHostName2.empty() ) {
			obs << (ByteStream::byte) 1;
			obs << hostconfig.HostName;
		}
		else
			obs << (ByteStream::byte) 0;

		sendStatusUpdate(obs, ADD_MODULE);
		log.writeLog(__LINE__, "reconfigureModule - module added from Shared Memory", LOG_TYPE_DEBUG);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "reconfigureModule - ERROR: sendStatusUpdate error", LOG_TYPE_ERROR);
		return API_FAILURE;
	}

	if ( moduleType == "pm" ) {
		if ( updatePMSconfig() != API_SUCCESS )
			return API_FAILURE;
	}

	//Update DBRM section of Calpont.xml
	if ( updateWorkerNodeconfig() != API_SUCCESS )
		return API_FAILURE;

	// remove all associated alarms for this modules being removed
	clearModuleAlarms( moduleName );

	//distribute config file
	distributeConfigFile("system");	

	return API_SUCCESS;
}


/******************************************************************************************
* @brief	sendMsgProcMon
*
* purpose:	Sends a Msg to ProcMon
*
******************************************************************************************/
int ProcessManager::sendMsgProcMon( std::string module, ByteStream msg, int requestID, int timeout )
{
	string msgPort;
	int returnStatus = API_MINOR_FAILURE;
	Oam oam;

	if ( module != config.moduleName() ) {
		msgPort = module + "_ProcessMonitor";

		// do a ping test to determine a quick failure
		Config* sysConfig = Config::makeConfig();
	
		string IPAddr = sysConfig->getConfig(msgPort, "IPAddr");
	
		if ( IPAddr == oam::UnassignedIpAddr ) {
			log.writeLog(__LINE__, "sendMsgProcMon ping failure", LOG_TYPE_ERROR);
			return returnStatus;
		}
	
		string cmdLine = "ping ";
		string cmdOption = " -c 1 -w 5 >> /dev/null";
		string cmd = cmdLine + IPAddr + cmdOption;
		if ( system(cmd.c_str()) != 0) {
			//ping failure
			log.writeLog(__LINE__, "sendMsgProcMon ping failure", LOG_TYPE_ERROR);
			return returnStatus;
		}
	}
	else
		// use the localhost IP Address
		msgPort = "localhost_ProcessMonitor";

	log.writeLog(__LINE__, "sendMsgProcMon: Process module " + module , LOG_TYPE_DEBUG);
	try
	{
		MessageQueueClient mqRequest(msgPort);
		mqRequest.write(msg);

		if ( timeout > 0 ) {
			// wait for response
			ByteStream::byte returnACK;
			ByteStream::byte returnRequestID;
			ByteStream::byte requestStatus;
			ByteStream receivedMSG;
		
			struct timespec ts = { timeout, 0 };

			// get current time in seconds
			time_t startTimeSec;
			time (&startTimeSec);

			while(true)
			{
				try {
					receivedMSG = mqRequest.read(&ts);
				}
				catch (SocketClosed &ex) {
					string error = ex.what();
					log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read, module " + module + " : " + error, LOG_TYPE_ERROR);
					return returnStatus;
				}
				catch (...) {
					log.writeLog(__LINE__, "EXCEPTION ERROR on mqRequest.read: Caught unknown exception! module " + module, LOG_TYPE_ERROR);
					return returnStatus;
				}
	
				if (receivedMSG.length() > 0) {
					receivedMSG >> returnACK;
					receivedMSG >> returnRequestID;
					receivedMSG >> requestStatus;
		
					if ( returnACK == oam::ACK &&  returnRequestID == requestID) {
						// ACK for this request
						returnStatus = requestStatus;
						break;	
					}	
					else
						log.writeLog(__LINE__, "sendMsgProcMon: invalid message " + module, LOG_TYPE_ERROR);
				}
				else
				{	//api timeout occurred, check if retry should be done
					// get current time in seconds
					time_t endTimeSec;
					time (&endTimeSec);
					if ( timeout <= (endTimeSec - startTimeSec) ) {
						log.writeLog(__LINE__, "sendMsgProcMon: ProcMon Msg timeout on module " + module, LOG_TYPE_ERROR);
						break;
					}
				}
			}
		}
		else
			returnStatus = oam::API_SUCCESS;

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
* @brief	sendMsgProcMon1
*
* purpose:	Sends a Msg to ProcMon
*
******************************************************************************************/
std::string ProcessManager::sendMsgProcMon1( std::string module, ByteStream msg, int requestID )
{
	string msgPort;
	string returnStatus = "FAILED";

	if ( module != config.moduleName() ) {
		msgPort = module + "_ProcessMonitor";

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
	}
	else
		// use the localhost IP Address
		msgPort = "localhost_ProcessMonitor";

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
		}
		else
			log.writeLog(__LINE__, "sendMsgProcMon1: ProcMon Msg timeout on module " + module, LOG_TYPE_ERROR);

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
* @brief	saveBRM
*
* purpose:	Execute the reset_locks then save BRM data script
*
******************************************************************************************/
void ProcessManager::saveBRM()
{
	Oam oam;
	int rtnCode = system("/usr/local/Calpont/bin/reset_locks  > /var/log/Calpont/reset_locks.log1 2>&1");
	if (rtnCode != 1) {
		log.writeLog(__LINE__, "Successfully Launched reset_locks", LOG_TYPE_DEBUG);
	}
	else
		log.writeLog(__LINE__, "Error Launching reset_locks", LOG_TYPE_ERROR);

	rtnCode = system("/usr/local/Calpont/bin/save_brm  > /var/log/Calpont/save_brm.log1 2>&1");
	if (rtnCode != 1) {
		log.writeLog(__LINE__, "Successfully Launched DBRM save_brm", LOG_TYPE_DEBUG);
	}
	else
		log.writeLog(__LINE__, "Error Launching DBRM save_brm", LOG_TYPE_ERROR);
}

/******************************************************************************************
* @brief	createModuleFile
*
* purpose:	Create a module file for remote server
*
******************************************************************************************/
bool ProcessManager::createModuleFile(string remoteModuleName)
{
	// Read Local Install flag

	string fileName = "/usr/local/Calpont/local/etc/" + remoteModuleName + "/module";

	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());

	string cmd = "echo " + remoteModuleName + " > " + fileName;
	system(cmd.c_str());

	newFile.close();

	return true;
}

/******************************************************************************************
* @brief	updateExports
*
* purpose:	Update pm exports file for remote Modules
*
******************************************************************************************/
bool ProcessManager::updateExports(std::string IPAddress)
{
	Oam oam;
	ByteStream msg;
	ByteStream::byte requestID = UPDATEEXPORTS;
	msg << requestID;
	msg << IPAddress;

	ModuleTypeConfig moduletypeconfig;
	oam.getSystemConfig("pm", moduletypeconfig);

	DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();
	for ( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
	{
		sendMsgProcMon( (*pt).DeviceName, msg, requestID );
	}

	return true;
}

/*****************************************************************************************
* @brief	startSystemThread
*
* purpose:	Send Messages to Module Process Monitors to start Processes
*
*****************************************************************************************/
void startSystemThread(oam::DeviceNetworkList devicenetworklist)
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	SystemModuleTypeConfig systemmoduletypeconfig;
	SNMPManager aManager;
	int status = API_SUCCESS;
	bool exitThread = false;
	int exitThreadStatus = oam::API_SUCCESS;

	log.writeLog(__LINE__, "startSystemThread launched", LOG_TYPE_DEBUG);

	// get system status and exit thread if in AUTO_INIT OR MAN_INIT
	SystemStatus systemstatus;
	try {
		oam.getSystemStatus(systemstatus);

		if (systemstatus.SystemOpState == AUTO_INIT ||
			 systemstatus.SystemOpState == MAN_INIT) {
			log.writeLog(__LINE__, "Start already in-progess, exit startSystemThread", LOG_TYPE_DEBUG);
			startsystemthreadStatus = oam::API_ALREADY_IN_PROGRESS;
			exitThread = true;
			exitThreadStatus = oam::API_ALREADY_IN_PROGRESS;
		}
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemStatus: " + error, LOG_TYPE_ERROR);
		startsystemthreadStatus = oam::API_FAILURE;
		processManager.setSystemState(oam::MAN_OFFLINE);
		exitThread = true;
		exitThreadStatus = oam::API_FAILURE;
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemStatus: Caught unknown exception!", LOG_TYPE_ERROR);
		startsystemthreadStatus = oam::API_FAILURE;
		processManager.setSystemState(oam::MAN_OFFLINE);
		exitThread = true;
		exitThreadStatus = oam::API_FAILURE;
	}

	if ( exitThread )
		pthread_exit((void*) exitThreadStatus);

	try{
		oam.getSystemConfig(systemmoduletypeconfig);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
		startsystemthreadStatus = oam::API_FAILURE;
		processManager.setSystemState(oam::MAN_OFFLINE);
		exitThread = true;
		exitThreadStatus = oam::API_FAILURE;
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		startsystemthreadStatus = oam::API_FAILURE;
		processManager.setSystemState(oam::MAN_OFFLINE);
		exitThread = true;
		exitThreadStatus = oam::API_FAILURE;
	}

	if ( exitThread )
		pthread_exit((void*) exitThreadStatus);

	if (systemstatus.SystemOpState == AUTO_OFFLINE)
		processManager.setSystemState(oam::AUTO_INIT);
	else
		processManager.setSystemState(oam::MAN_INIT);

	startsystemthreadRunning = true;

	string newStandbyModule = processManager.getStandbyModule();

	if ( !newStandbyModule.empty() && newStandbyModule != "NONE")
		processManager.setStandbyModule(newStandbyModule);

	//distribute config file
	processManager.distributeConfigFile("system");

	//configure PMS ports
	if ( processManager.updatePMSconfig(true) != API_SUCCESS )
		pthread_exit((void*) oam::API_FAILURE);

	if ( devicenetworklist.size() != 0 ) {
		// start modules from devicenetworklist
		DeviceNetworkList::iterator listPT = devicenetworklist.begin();

		//launch start module threads, starting with local module                
		pthread_t startmodulethread;                
		string moduleName = config.moduleName();                
		int status = pthread_create (&startmodulethread, NULL, (void*(*)(void*)) &startModuleThread, &moduleName);                
		
		if ( status != 0 )
			log.writeLog(__LINE__, "startModuleThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

		sleep(5);	

		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			string moduleName = (*listPT).DeviceName;

			// skip local module name
			if ( moduleName == config.moduleName() )
				continue;

			// bypass DISABLED modules
			try{
				int opState;
				bool degraded;
				oam.getModuleStatus(moduleName, opState, degraded);

				if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
					//skip
					continue;
			}
			catch (exception& ex)
			{
				string error = ex.what();
				log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": " + error, LOG_TYPE_ERROR);
			}
			catch(...)
			{
				log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": Caught unknown exception!", LOG_TYPE_ERROR);
			}

			if ( startsystemthreadStop ) {
				log.writeLog(__LINE__, "startSystemThread exit early, startsystemthreadStop set", LOG_TYPE_DEBUG);
				startsystemthreadStatus = oam::API_FAILURE;
				processManager.setSystemState(oam::MAN_OFFLINE);
				startsystemthreadRunning = false;
				pthread_exit((void*) oam::API_FAILURE);
			}

			pthread_t startmodulethread;
			int status = pthread_create (&startmodulethread, NULL, (void*(*)(void*)) &startModuleThread, &moduleName);
			
			if ( status != 0 )
				log.writeLog(__LINE__, "startModuleThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

			sleep(3);
		}
	}
	else {
		// start all modules, like on a systemStart command
		//launch start module threads, starting with local module

		pthread_t startmodulethread;
		string moduleName = config.moduleName();
		int status = pthread_create (&startmodulethread, NULL, (void*(*)(void*)) &startModuleThread, &moduleName);

		if ( status != 0 )
			log.writeLog(__LINE__, "startModuleThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

		sleep(3);

		for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
		{
			if ( startsystemthreadStop ) {
				log.writeLog(__LINE__, "startSystemThread exit early, startsystemthreadStop set", LOG_TYPE_DEBUG);
				startsystemthreadStatus = oam::API_FAILURE;
				processManager.setSystemState(oam::MAN_OFFLINE);
				startsystemthreadRunning = false;
				pthread_exit((void*) oam::API_FAILURE);
			}
	
			int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
			if( moduleCount == 0)
				continue;
	
			DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
			{
				string moduleName = (*pt).DeviceName;
	
				// skip local module name
				if ( moduleName == config.moduleName() )
					continue;

				// bypass DISABLED modules
				try{
					int opState;
					bool degraded;
					oam.getModuleStatus(moduleName, opState, degraded);
	
					if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
						//skip
						continue;
	
				}
				catch (exception& ex)
				{
					string error = ex.what();
					log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": " + error, LOG_TYPE_ERROR);
				}
				catch(...)
				{
					log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": Caught unknown exception!", LOG_TYPE_ERROR);
				}

				if ( startsystemthreadStop ) {
					log.writeLog(__LINE__, "startSystemThread exit early, startsystemthreadStop set", LOG_TYPE_DEBUG);
					startsystemthreadStatus = oam::API_FAILURE;
					processManager.setSystemState(oam::MAN_OFFLINE);
					startsystemthreadRunning = false;
					pthread_exit((void*) oam::API_FAILURE);
				}

				pthread_t startmodulethread;
				int status = pthread_create (&startmodulethread, NULL, (void*(*)(void*)) &startModuleThread, &moduleName);

				if ( status != 0 )
					log.writeLog(__LINE__, "startModuleThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

				sleep(3);
			}
		}
	}

	// check status and process accordingly
	int k = 0;
	for( ; k < 100 ; k++ )
	{
		if ( startsystemthreadStop ) {
			log.writeLog(__LINE__, "startSystemThread exit early, startsystemthreadStop set", LOG_TYPE_DEBUG);
			if ( startmodulethreadStatus != API_SUCCESS ) {
				startsystemthreadStatus = startmodulethreadStatus;
				processManager.setSystemState(oam::FAILED);
			}
			else
			{
				startsystemthreadStatus = API_FAILURE;
				processManager.setSystemState(oam::MAN_OFFLINE);
			}
			startsystemthreadRunning = false;
			pthread_exit((void*) oam::API_FAILURE);
		}

		string moduleName;
		status = API_SUCCESS;
		for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
		{
			int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
			if( moduleCount == 0)
				continue;
			DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
			{
				moduleName = (*pt).DeviceName;
	
				// get module status
				try{
					int opState;
					bool degraded;
					oam.getModuleStatus(moduleName, opState, degraded);
	
					if ( opState == oam::FAILED ) {
						if ( startmodulethreadStatus != API_SUCCESS )
							status = startmodulethreadStatus;
						else
							status = API_FAILURE;
						break;
					}
	
					if (opState == oam::ACTIVE ||
						opState == oam::MAN_DISABLED ||
						opState == oam::AUTO_DISABLED ||
						(opState == oam::MAN_OFFLINE && k > 0) )
						//skip
						continue;

					status = API_ALREADY_IN_PROGRESS;
				}
				catch (exception& ex)
				{
					string error = ex.what();
					log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": " + error, LOG_TYPE_ERROR);
					continue;
				}
				catch(...)
				{
					log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": Caught unknown exception!", LOG_TYPE_ERROR);
					continue;
				}
			}
			if( status == API_FAILURE )
				break;
		}

		//get out of loop if all modules started successfully
		if( status == API_SUCCESS ) {
			//send message to start new Standby Process-Manager, if needed
			string newStandbyModule = processManager.getStandbyModule();
		
			if ( !newStandbyModule.empty() && newStandbyModule != "NONE")
				// get standby IP address and update entries
				processManager.setStandbyModule(newStandbyModule);

			//distribute config file
			processManager.distributeConfigFile("system");
//			sleep(2);

			processManager.setSystemState(oam::ACTIVE);

			break;
		}
		else
		{
			//get out of loop if start module failed
			if( status == API_FAILURE ) {
				//set system status
				log.writeLog(__LINE__, "startSystemThread: Module failed, Set System State to FAILED: " + moduleName , LOG_TYPE_CRITICAL);
				processManager.setSystemState(oam::FAILED);
				break;
			}
		}
		sleep(5);
	}
	
	if ( k == 100 ) {
		// system didn't successfull restart
		log.writeLog(__LINE__, "startSystemThread: Modules failed to start after 100 tries, Set System State to FAILED" , LOG_TYPE_CRITICAL);
		processManager.setSystemState(oam::FAILED);
		status = oam::API_FAILURE;
	}

	// exit thread
	log.writeLog(__LINE__, "startSystemThread Exit", LOG_TYPE_DEBUG);
	startsystemthreadStatus = status;
	startsystemthreadRunning = false;
	pthread_exit(0);
}

/*****************************************************************************************
* @brief	startModuleThread
*
* purpose:	Send Messages to Module Process Monitors to start Processes
*
*****************************************************************************************/
void startModuleThread(string module)
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	bool exitThread = false;
	int exitThreadStatus = oam::API_SUCCESS;

	//store in a local variable
	string moduleName = module;
	if ( moduleName.empty() ){
		log.writeLog(__LINE__, "startModuleThread received on invalid module name", LOG_TYPE_ERROR);
		pthread_exit(0);
	}

	log.writeLog(__LINE__, "Start Module " + moduleName, LOG_TYPE_DEBUG);

	bool start = false;
	while(true)
	{
		// getsystem status
		SystemStatus systemstatus;
		try {
			oam.getSystemStatus(systemstatus);
		}
		catch (exception& ex) {
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemStatus: " + error, LOG_TYPE_ERROR);
			exitThread = true;
			exitThreadStatus = oam::API_FAILURE;
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemStatus: Caught unknown exception!", LOG_TYPE_ERROR);
			exitThread = true;
			exitThreadStatus = oam::API_FAILURE;
		}

	if ( exitThread )
		pthread_exit((void*) exitThreadStatus);

		if ( startsystemthreadStop) {
			// set status and exit this thread
			processManager.setModuleState(moduleName, oam::MAN_OFFLINE);
			log.writeLog(__LINE__, "startModuleThread early exit on " + moduleName, LOG_TYPE_DEBUG);
			pthread_exit(0);
		}

		// get module status
		uint16_t startType = oam::MAN_OFFLINE;
		try{
			int opState;
			bool degraded;
			oam.getModuleStatus(moduleName, opState, degraded);

			if ( opState == oam::AUTO_OFFLINE || opState == oam::AUTO_INIT)
				startType = oam::AUTO_OFFLINE;

			if (opState == oam::ACTIVE ||
				opState == oam::MAN_DISABLED ||
				opState == oam::AUTO_DISABLED ||
				( opState == oam::MAN_OFFLINE && start) )
				//quit
				break;

			start = true;
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": Caught unknown exception!", LOG_TYPE_ERROR);
		}

		int retStatus = processManager.startModule(moduleName, oam::FORCEFUL, startType, true);
	
		log.writeLog(__LINE__, "ACK received from '" + moduleName + "' Process-Monitor, return status = " + oam.itoa(retStatus), LOG_TYPE_DEBUG);
	
		if (retStatus == API_SUCCESS)
			break;
		else
		{
			if (retStatus != API_MINOR_FAILURE) {
				//major failure, set stopsystem flag and exit this thread
				startmodulethreadStatus = retStatus;
				startsystemthreadStop = true;
				break;
			}
		}
	}

	// exit thread
	log.writeLog(__LINE__, "startModuleThread Exit on " + moduleName, LOG_TYPE_DEBUG);
	pthread_exit(0);
}


/*****************************************************************************************
* @brief	checkSimplexModule
*
* purpose:	Check for simplex module run-type and start mate processes if needed
*
*****************************************************************************************/
void ProcessManager::checkSimplexModule(std::string moduleName)
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	SystemModuleTypeConfig systemmoduletypeconfig;
	SystemProcessConfig systemprocessconfig;

	log.writeLog(__LINE__, "checkSimplexModule called for " + moduleName, LOG_TYPE_DEBUG);

	try{
		oam.getSystemConfig(systemmoduletypeconfig);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
		return;
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		return;
	}

	string moduletype = moduleName.substr(0,MAX_MODULE_TYPE_SIZE);

	for( unsigned int i = 0; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
	{
		if ( moduletype == systemmoduletypeconfig.moduletypeconfig[i].ModuleType ) {

			if( systemmoduletypeconfig.moduletypeconfig[i].ModuleCount == 0)
				return;

			//check for SIMPLEX Processes on mate might need to be started
			if( systemmoduletypeconfig.moduletypeconfig[i].RunType == SIMPLEX ) {

				DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
				for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
				{
					if ((*pt).DeviceName != moduleName) {
					//mate module, check for module ACTIVE and SIMPLEX processes
						int opState;
						try{
							bool degraded;
							oam.getModuleStatus((*pt).DeviceName, opState, degraded);

							if (opState == oam::ACTIVE ) {
								//start COLD_STANDBY processes
								try {
									oam.getProcessConfig(systemprocessconfig);

									for( unsigned int j = 0 ; j < systemprocessconfig.processconfig.size(); j++)
									{
										if ( systemprocessconfig.processconfig[j].ModuleType == moduletype &&
											systemprocessconfig.processconfig[j].RunType == oam::SIMPLEX ) {
											int state;
											try{
												ProcessStatus procstat;
												oam.getProcessStatus(systemprocessconfig.processconfig[j].ProcessName,
																		(*pt).DeviceName, procstat);
												state = procstat.ProcessOpState;
											}
											catch (exception& ex)
											{
												string error = ex.what();
												log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
												continue;
											}
											catch(...)
											{
												log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
												continue;
											}
											if ( state == oam::COLD_STANDBY ) {
												int status = processManager.startProcess((*pt).DeviceName,
																	systemprocessconfig.processconfig[j].ProcessName, 
																	FORCEFUL);
												if ( status == API_SUCCESS ) {
													log.writeLog(__LINE__, "checkSimplexModule: mate process started: " + (*pt).DeviceName + "/" + systemprocessconfig.processconfig[j].ProcessName, LOG_TYPE_DEBUG);
//													sleep(4);

													//check to see if DDL/DML IPs need to be updated
													if ( (*pt).DeviceName.find("pm") == 0  && 			systemprocessconfig.processconfig[j].ProcessName == "DDLProc" ) {
														setPMProcIPs((*pt).DeviceName);

														//distribute config file
												//		distributeConfigFile("system");	
													}
												}
												else
													log.writeLog(__LINE__, "checkSimplexModule: mate process failed to start: " + (*pt).DeviceName + "/" + systemprocessconfig.processconfig[j].ProcessName, LOG_TYPE_DEBUG);
											}
											else
											{ // if found ACTIVE, skip to next process
												if ( state == oam::ACTIVE )
													return;
											}
										}
									}
								}
								catch (exception& ex)
								{
									string error = ex.what();
									log.writeLog(__LINE__, "checkSimplexModule: EXCEPTION ERROR on getProcessConfig: " + error, LOG_TYPE_ERROR);
								}
								catch(...)
								{
									log.writeLog(__LINE__, "checkSimplexModule: EXCEPTION ERROR on getProcessConfig: Caught unknown exception!", LOG_TYPE_ERROR);
								}
							}
						}
						catch (exception& ex)
						{
							string error = ex.what();
							log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": " + error, LOG_TYPE_ERROR);
						}
						catch(...)
						{
							log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": Caught unknown exception!", LOG_TYPE_ERROR);
						}
					}
				}
			}
		}
		return;	
	}
}

/******************************************************************************************
* @brief	updatePMSconfig
*
* purpose:	Update PMS Configuration in System Configuration file
*
******************************************************************************************/
int ProcessManager::updatePMSconfig( bool check )
{
	Oam oam;
	int pmPorts = 32;
	vector<string> IpAddrs;
	vector<int> nicIDs;

	log.writeLog(__LINE__, "updatePMSconfig Started", LOG_TYPE_DEBUG);

	pthread_mutex_lock(&THREAD_LOCK);

	ModuleTypeConfig moduletypeconfig;
	oam.getSystemConfig("pm", moduletypeconfig);

	Config* sysConfig = Config::makeConfig();
	string pmsIPAddr = sysConfig->getConfig("PMS1", "IPAddr");

	//exit out if PMS already setup
	if( pmsIPAddr != oam::UnassignedIpAddr && 
		check)
	{
		log.writeLog(__LINE__, "updatePMSconfig: no update needed, exiting function", LOG_TYPE_DEBUG);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_SUCCESS;
	}

	//exit out if PM module count is 1 or less
	if( moduletypeconfig.ModuleCount <= 1 && 
		check)
	{
		log.writeLog(__LINE__, "updatePMSconfig: no update needed, exiting function", LOG_TYPE_DEBUG);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_SUCCESS;
	}

	int maxPMNicID = atoi(sysConfig->getConfig("PrimitiveServers", "ConnectionsPerPrimProc").c_str()) / 2;
	int pmCount = 0;

	//get Perfomance module IP addresses
	DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();
	for ( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
	{
		int opState;
		bool degraded;
		try {
			oam.getModuleStatus((*pt).DeviceName, opState, degraded);

			if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
				continue;
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": Caught unknown exception!", LOG_TYPE_ERROR);
		}

		pmCount++;

		HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
		for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
		{
			if ( (*pt1).IPAddr == oam::UnassignedIpAddr )
				continue;
			else
			{
				//check NIC status and don't assigned if down
				try{
					int state;
					oam.getNICStatus((*pt1).HostName, state);
					if ( state == oam::UP || state == oam::INITIAL) {
						IpAddrs.push_back((*pt1).IPAddr);
						nicIDs.push_back((*pt1).NicID);
					}
				}
				catch (...)
				{
					IpAddrs.push_back((*pt1).IPAddr);
					nicIDs.push_back((*pt1).NicID);
				}
			}
		}
	}

	if( IpAddrs.empty()) {
		log.writeLog(__LINE__, "updatePMSconfig: No up NICS found, exiting function", LOG_TYPE_DEBUG);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_SUCCESS;
	}
 
	if( pmCount == 0) {
		log.writeLog(__LINE__, "updatePMSconfig: No PM modules Enabled, exiting function", LOG_TYPE_DEBUG);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_SUCCESS;
	}

	if( pmCount == 1 && 
		pmsIPAddr != oam::UnassignedIpAddr && 
		check )
	{
		log.writeLog(__LINE__, "updatePMSconfig: no update needed, exiting function", LOG_TYPE_DEBUG);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_SUCCESS;
	}

	Configuration config;

	for(int i=0 ; i < 5; i++)
	{
		Config* sysConfig1 = Config::makeConfig();

		//update PM count if needed
		sysConfig1->setConfig("PrimitiveServers", "Count", oam.itoa(pmCount));
	
		const string PM = "PMS";
		int nicID = 1;

		for ( int pmsID = 1;  pmsID < pmPorts+1 ; )
		{
			vector<string>::iterator pt = IpAddrs.begin();
			vector<int>::iterator pt1 = nicIDs.begin();

			for( ; pt != IpAddrs.end() ; pt++,pt1++)
			{
				if ( *pt1 == nicID ) {
					string pmsName = PM + oam.itoa(pmsID);
					sysConfig1->setConfig(pmsName, "IPAddr", *pt);
					pmsID++;
				}
				if ( pmsID > pmPorts )
					break;
			}

			if ( pmsID > pmPorts )
				break;

			nicID++;
			if ( nicID > maxPMNicID )
				nicID = 1;
		}
	
		//update Calpont Config table
		try {
			sysConfig1->write();
			pthread_mutex_unlock(&THREAD_LOCK);
			
			log.writeLog(__LINE__, "updatePMSconfig completed", LOG_TYPE_DEBUG);

			return API_SUCCESS;
		}
		catch(...)
		{
			log.writeLog(__LINE__, "updatePMSconfig - ERROR: sysConfig->write", LOG_TYPE_ERROR);
		}
	}

	pthread_mutex_unlock(&THREAD_LOCK);
	
	log.writeLog(__LINE__, "updatePMSconfig failed", LOG_TYPE_DEBUG);

	return API_FAILURE;
}

/******************************************************************************************
* @brief	updateWorkerNodeconfig
*
* purpose:	Update WorkerNode Configuration in System Configuration file
*
******************************************************************************************/
int ProcessManager::updateWorkerNodeconfig()
{
	Oam oam;
	vector <string> module;
	vector <string> ipadr;

	log.writeLog(__LINE__, "updateWorkerNodeconfig Started", LOG_TYPE_DEBUG);

	pthread_mutex_lock(&THREAD_LOCK);

	//setup current module as work-node #1 by entering it in first
	module.push_back(config.moduleName());

	// get my IP address and update entries
	ModuleConfig moduleconfig;
	oam.getSystemConfig(config.moduleName(), moduleconfig);
	HostConfigList::iterator pt0 = moduleconfig.hostConfigList.begin();
	assert(pt0 != moduleconfig.hostConfigList.end());
	ipadr.push_back(pt0->IPAddr);

	SystemModuleTypeConfig systemmoduletypeconfig;

	try
	{
		oam.getSystemConfig(systemmoduletypeconfig);

		for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
		{
			if( systemmoduletypeconfig.moduletypeconfig[i].ModuleType.empty() )
				// end of list
				break;

			int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;

			if ( moduleCount > 0 )
			{
				DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
				for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
				{
					//skip current module
					if ( (*pt).DeviceName == config.moduleName() )
						continue;

					int opState;
					bool degraded;
					try {
						oam.getModuleStatus((*pt).DeviceName, opState, degraded);

						if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED)
							continue;
					}
					catch (exception& ex)
					{
						string error = ex.what();
						log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": " + error, LOG_TYPE_ERROR);
					}
					catch(...)
					{
						log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": Caught unknown exception!", LOG_TYPE_ERROR);
					}
			
					module.push_back((*pt).DeviceName);

					HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
					ipadr.push_back((*pt1).IPAddr);
				}
			}
		}
	}
	catch (...)
	{
		log.writeLog(__LINE__, "updateWorkerNodeconfig: getSystemNetworkConfig Failed", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_SUCCESS;
	}

	Configuration config;

	for ( int i = 1 ; i < 5 ; i++ )
	{
		Config* sysConfig3 = Config::makeConfig();;
	
		//update Calpont.xml
		sysConfig3->setConfig("DBRM_Controller", "NumWorkers", oam.itoa(module.size()));
	
		std::vector<std::string>::iterator pt = module.begin();
		std::vector<std::string>::iterator pt1 = ipadr.begin();
		int id = 1;
		for( ; pt != module.end() ; pt++,pt1++,id++)
		{
			string Section = "DBRM_Worker" + oam.itoa(id);
			sysConfig3->setConfig(Section, "IPAddr", *pt1);
			sysConfig3->setConfig(Section, "Module", *pt);
			string moduleName = *pt;
			if ( moduleName.find("xm",0) == 0 ) {
				string systemID = "00";			
				try {
					string BRM_UID = sysConfig3->getConfig("ExtentMap", "BRM_UID");
			
					systemID = BRM_UID.substr(2,2);;
					if ( systemID.size() == 1 )
						systemID = "0" + systemID;
				}
				catch(...)
				{
					log.writeLog(__LINE__, "ERROR: Problem getting BRM_UID in the Calpont System Configuration file", LOG_TYPE_ERROR);
				}
				sysConfig3->setConfig(Section, "Port", "87" + systemID);
			}
			else
				sysConfig3->setConfig(Section, "Port", "8700");
		}
	
		//clear out any leftovers
		for ( ; id < MAX_MODULE ; id++ )
		{
			string Section = "DBRM_Worker" + oam.itoa(id);
			
			if ( sysConfig3->getConfig(Section, "IPAddr") != oam::UnassignedIpAddr &&
					!sysConfig3->getConfig(Section, "IPAddr").empty())
				sysConfig3->setConfig(Section, "IPAddr", oam::UnassignedIpAddr);
			if ( sysConfig3->getConfig(Section, "Module") != oam::UnassignedIpAddr &&
					!sysConfig3->getConfig(Section, "Module").empty())
				sysConfig3->setConfig(Section, "Module", oam::UnassignedName);
		}
	
		try {
			sysConfig3->write();
			pthread_mutex_unlock(&THREAD_LOCK);

			log.writeLog(__LINE__, "updateWorkerNodeconfig completed", LOG_TYPE_DEBUG);

			return API_SUCCESS;

		}
		catch(...)
		{
			log.writeLog(__LINE__, "updateWorkerNodeconfig - ERROR: sysConfig->write", LOG_TYPE_ERROR);
		}
	}

	pthread_mutex_unlock(&THREAD_LOCK);
	log.writeLog(__LINE__, "updateWorkerNodeconfig failed", LOG_TYPE_DEBUG);

	return API_FAILURE;
}

/******************************************************************************************
* @brief	clearModuleAlarms
*
* purpose:	Clears all alarms related to a module
*
******************************************************************************************/
void ProcessManager::clearModuleAlarms(std::string moduleName)
{
	SNMPManager aManager;
	AlarmList alarmList;
	aManager.getActiveAlarm (alarmList);

	AlarmList::iterator i;
	for (i = alarmList.begin(); i != alarmList.end(); ++i)
	{
		// check if the same fault component on same module
		if (moduleName.compare((i->second).getComponentID()) == 0 ||
			moduleName.compare((i->second).getSname()) == 0)
		{
			// match, go clear it
			aManager.sendAlarmReport((i->second).getComponentID().c_str(),
									(i->second).getAlarmID(),
									CLEAR,
									(i->second).getSname().c_str(),
									"ProcessManager");
		}
	}
}

/******************************************************************************************
* @brief	clearNICAlarms
*
* purpose:	Clears all alarms related to a NIC hostName
*
******************************************************************************************/
void ProcessManager::clearNICAlarms(std::string hostName)
{
	SNMPManager aManager;
	AlarmList alarmList;
	aManager.getActiveAlarm (alarmList);

	AlarmList::iterator i;
	for (i = alarmList.begin(); i != alarmList.end(); ++i)
	{
		// check if the same fault component on same module
		if (hostName.compare((i->second).getComponentID()) == 0)
		{
			// match, go clear it
			aManager.sendAlarmReport((i->second).getComponentID().c_str(),
									(i->second).getAlarmID(),
									CLEAR,
									(i->second).getSname().c_str(),
									"ProcessManager");
		}
	}
}

/******************************************************************************************
* @brief	updateExtentMap
*
* purpose:	update Extent Map section in Calpont.xml
*
******************************************************************************************/
bool ProcessManager::updateExtentMap()
{
	string fileName = "/usr/local/Calpont/etc/Calpont.xml";

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return false;
	
	vector <string> lines;
	char line[200];
	string buf;
	string newLine;

	string start = "</Installation>";
	string firstComment = "<!--";
	string end = "</ExtentMap>";
	string lastComment = "-->";

	while (oldFile.getline(line, 200))
	{
		buf = line;

		string::size_type pos = buf.find(start,0);
		if (pos != string::npos)
		{
			//output to temp file and skip next line
			lines.push_back(buf);
			oldFile.getline(line, 200);
			buf = line;
			pos = buf.find(firstComment,0);
			if (pos == string::npos)
			{
				return true;
			}
		}
		else
		{
			pos = buf.find(end,0);
			if (pos != string::npos)
			{
				//output to temp file and skip next line
				lines.push_back(buf);
				oldFile.getline(line, 200);
				buf = line;
				pos = buf.find(lastComment,0);
				if (pos == string::npos)
				{
					return true;
				}
			}
			else
				//output to temp file
				lines.push_back(buf);
		}
	}

	oldFile.close();
	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());	
	
	//create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	close(fd);
	return true;
}

/******************************************************************************************
* @brief	makeXMInittab
*
* purpose:	Make inittab to auto-launch ProcMon
*
******************************************************************************************/
bool ProcessManager::makeXMInittab(std::string moduleName, std::string systemID, std::string parentOAMModuleHostName)
{
	string fileName = "/usr/local/Calpont/local/etc/" + moduleName + "/inittab.calpont";

	vector <string> lines;

	string init1 = "1" + systemID + ":2345:respawn:/usr/local/Calpont/bin/ProcMon " + parentOAMModuleHostName;
	
	lines.push_back(init1);

	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());	
	
	//create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	close(fd);

	return true;
}

/******************************************************************************************
* @brief	setXMmount
*
* purpose:	setup External Module mount file
*
******************************************************************************************/
bool ProcessManager::setXMmount(std::string moduleName, std::string parentOAMModuleHostName, std::string parentOAMModuleIPAddr)
{
	//
	// update xm-mount file in /usr/local/Calpont/local/etc
	//
	string fileName = "/usr/local/Calpont/local/etc/" + moduleName + "/" + parentOAMModuleHostName + "_mount";

	vector <string> lines;

	string mount1 = "umount -fl /mnt/" + parentOAMModuleHostName + "_etc";
	string mount2 = "umount -fl /mnt/" + parentOAMModuleHostName + "_OAM";
	string mount3 = "mount -t nfs -o ro " + parentOAMModuleIPAddr + ":/usr/local/Calpont/etc /mnt/" + parentOAMModuleHostName + "_etc";
	string mount4 = "mount -t nfs -o ro " + parentOAMModuleIPAddr + ":/mnt/OAM /mnt/" + parentOAMModuleHostName + "_OAM";

	//add Calpont mount data 
	lines.push_back(mount1);
	lines.push_back(mount2);
	lines.push_back(mount3);
	lines.push_back(mount4);

	unlink (fileName.c_str());
	ofstream newFile (fileName.c_str());	
	
	//create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	close(fd);

	return true;
}

/******************************************************************************************
* @brief	setPMProcIPs
*
* purpose:	Updates the Calpont.xml file for DDL/DMLProc IPs during PM switchover
*
*
******************************************************************************************/
int ProcessManager::setPMProcIPs( std::string moduleName )
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	ModuleConfig moduleconfig;

	log.writeLog(__LINE__, "setPMProcIPs called for " + moduleName, LOG_TYPE_DEBUG);

	pthread_mutex_lock(&THREAD_LOCK);

	for ( int i = 1 ; i < 5 ; i ++)
	{
		//get Module IP address
		try
		{
			oam.getSystemConfig(moduleName, moduleconfig);
			HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
			string ipAdd = (*pt1).IPAddr;
	
			Config* sysConfig2 = Config::makeConfig();
		
			//check if IP address if different than current value, don't update if it is
			if ( sysConfig2->getConfig("DDLProc", "IPAddr") == ipAdd &&
					sysConfig2->getConfig("DMLProc", "IPAddr") == ipAdd ) {
				log.writeLog(__LINE__, "setPMProcIPs: no update needed, exiting function", LOG_TYPE_DEBUG);	
				pthread_mutex_unlock(&THREAD_LOCK);
				return API_SUCCESS;
			}	
	
			sysConfig2->setConfig("DDLProc", "IPAddr", ipAdd);
			sysConfig2->setConfig("DMLProc", "IPAddr", ipAdd);
			try {
				sysConfig2->write();

				pthread_mutex_unlock(&THREAD_LOCK);

				log.writeLog(__LINE__, "setPMProcIPs to " + ipAdd, LOG_TYPE_DEBUG);
			}
			catch(...)
			{
				log.writeLog(__LINE__, "setPMProcIPs - ERROR: sysConfig->write", LOG_TYPE_ERROR);
			}
	
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "setPMProcIPs: EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "setPMProcIPs: EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		}
	}

	pthread_mutex_unlock(&THREAD_LOCK);

	log.writeLog(__LINE__, "setPMProcIPs failed", LOG_TYPE_DEBUG);

	return API_SUCCESS;

}

/******************************************************************************************
* @brief	distributeConfigFile
*
* purpose:	Distribute Calpont Config File to system modules
*
******************************************************************************************/
int ProcessManager::distributeConfigFile(std::string name, std::string file)
{
	ByteStream msg;
	ByteStream::byte requestID = UPDATECONFIGFILE;
	Oam oam;
	int returnStatus = oam::API_SUCCESS;

	msg << requestID;

	string fileName = "/usr/local/Calpont/etc/" + file;

	ifstream File (fileName.c_str());
	if (!File) {
		log.writeLog(__LINE__, "distributeConfigFile failed, file doesn't exist: " + fileName, LOG_TYPE_ERROR);
		return oam::API_FAILURE;
	}

	msg << fileName;

	char line[200];
	string buf;
	string calpontFile;

	while (File.getline(line, 200))
	{
		buf = line;
		calpontFile = calpontFile + buf + '\n';
	}

	msg << calpontFile;

	File.close();

	SystemModuleTypeConfig systemmoduletypeconfig;

	try{
		oam.getSystemConfig(systemmoduletypeconfig);
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

	if ( name == "system" ) {
		// send config file to all modules
		for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
		{
			int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
			if( moduleCount == 0)
				continue;
	
			DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
			{
				//skip local module
				if ( (*pt).DeviceName == config.moduleName() )
					continue;

				returnStatus = sendMsgProcMon( (*pt).DeviceName, msg, requestID, 0 );
			
				if ( returnStatus == API_SUCCESS)
				{
					//log the success event 
					log.writeLog(__LINE__, (*pt).DeviceName + " distributeConfigFile success.", LOG_TYPE_DEBUG);
				}
				else
				{
					//log the error event 
					log.writeLog(__LINE__, (*pt).DeviceName + " distributeConfigFile failed!!", LOG_TYPE_ERROR);
				}
			}
		}
	}
	else
	{
		returnStatus = sendMsgProcMon( name, msg, requestID, 0 );
	
		if ( returnStatus == API_SUCCESS)
		{
			//log the success event 
			log.writeLog(__LINE__, name + " distributeConfigFile success.", LOG_TYPE_DEBUG);
		}
		else
		{
			//log the error event 
			log.writeLog(__LINE__, name + " distributeConfigFile failed!!", LOG_TYPE_ERROR);
		}
	}

	return returnStatus;
}

/******************************************************************************************
* @brief	getDBRMData
*
* purpose:	get DBRM Data and send to requester
*
******************************************************************************************/
int ProcessManager::getDBRMData(messageqcpp::IOSocket fIos)
{
	ByteStream msg;
	Oam oam;
	int returnStatus = oam::API_SUCCESS;

	pthread_mutex_lock(&THREAD_LOCK);

	messageqcpp::IOSocket cfIos = fIos;

	string DBRMroot;
	oam.getSystemConfig("DBRMRoot", DBRMroot);
	string currentFileName = DBRMroot + "_current";
	string journalFileName = DBRMroot + "_journal";


	string oidFile;
	oam.getSystemConfig("OIDBitmapFile", oidFile);

	string currentDbrmFile;
	ifstream oldFile (currentFileName.c_str());
	if (oldFile) {
		// current file found, check for OIDBitmapFile
		ifstream mapFile (oidFile.c_str());
		if (!mapFile) {
			// no OIDBitmapFile, with current file, dbrm files are hosed
			log.writeLog(__LINE__, "getDBRMData: DBRM data files error, current file exist without OIDBitmapFile", LOG_TYPE_CRITICAL);
			pthread_mutex_unlock(&THREAD_LOCK);
			return oam::API_FAILURE_DB_ERROR;
		}

		char line[200];
		oldFile.getline(line, 200);
		currentDbrmFile = line;
	}
	else
	{
		log.writeLog(__LINE__, "getDBRMData: no DBRM current file found, must be initial install", LOG_TYPE_ERROR);

		msg << "initial";
		try {
			cfIos.write(msg);
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: Unknow exception", LOG_TYPE_ERROR);
			returnStatus = oam::API_FAILURE;
		}

		pthread_mutex_unlock(&THREAD_LOCK);
		return returnStatus;
	}

	string fileName = "/usr/local/Calpont/local/dbrmfiles";
	unlink(fileName.c_str());

	string cmd = "ls " + currentDbrmFile + "_* >> /usr/local/Calpont/local/dbrmfiles";
	system(cmd.c_str());

	ifstream file (fileName.c_str());
	if (!file) {
		log.writeLog(__LINE__, "getDBRMData: no DBRM files found, must be initial install", LOG_TYPE_ERROR);

		msg << "initial";
		try {
			cfIos.write(msg);
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: Unknow exception", LOG_TYPE_ERROR);
			returnStatus = oam::API_FAILURE;
		}

		pthread_mutex_unlock(&THREAD_LOCK);
		return returnStatus;
	}
	
	vector <string> dbrmFiles;

	char line[200];
	string buf;
	while (file.getline(line, 200))
	{
		buf = line;
		dbrmFiles.push_back(buf);
	}

	file.close();

	if ( dbrmFiles.size() < 1 ) {
		log.writeLog(__LINE__, "getDBRMData: dbrmFiles size = 0, must be initial install", LOG_TYPE_ERROR);

		msg << "initial";
		try {
			cfIos.write(msg);
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: Unknow exception", LOG_TYPE_ERROR);
			returnStatus = oam::API_FAILURE;
		}

		pthread_mutex_unlock(&THREAD_LOCK);
		return returnStatus;
	}

	// put oid file and current file in list
	dbrmFiles.push_back(currentFileName);

	ifstream file1 (journalFileName.c_str());
	if (file1)
		dbrmFiles.push_back(journalFileName);

	ifstream file2 (oidFile.c_str());
	if (file2)
		dbrmFiles.push_back(oidFile);

	//type
	msg << "files";
	try {
		cfIos.write(msg);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: Unknow exception", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return oam::API_FAILURE;
	}

	//remove any file of size 0
	std::vector<std::string>::iterator pt1 = dbrmFiles.begin();
	for( ; pt1 != dbrmFiles.end() ; pt1++)
	{
		string fileName = *pt1;
		ifstream in(fileName.c_str());

		in.seekg(0, std::ios::end);
		int size = in.tellg();
		if ( size == 0 )
			dbrmFiles.erase(pt1);
	}

	ByteStream fcmsg;

	// number of files
	fcmsg << (ByteStream::byte) dbrmFiles.size();
	try {
		cfIos.write(fcmsg);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: Unknow exception", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return oam::API_FAILURE;
	}

	pt1 = dbrmFiles.begin();
	for( ; pt1 != dbrmFiles.end() ; pt1++)
	{
		ByteStream fnmsg,fdmsg;

		string fileName = *pt1;
		ifstream in(fileName.c_str());

		//skip any file of size 0
		in.seekg(0, std::ios::end);
		int size = in.tellg();
		if ( size == 0 )
			continue;

		in.seekg(0, std::ios::beg);

		log.writeLog(__LINE__, fileName, LOG_TYPE_DEBUG);
		fnmsg << fileName;
		try {
			cfIos.write(fnmsg);
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: Unknow exception", LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return oam::API_FAILURE;
		}

		in >> fdmsg;
		try {
			cfIos.write(fdmsg);
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: Unknow exception", LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return oam::API_FAILURE;
		}
	}

	try {
		cfIos.write(msg);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on cfIos.write: Unknow exception", LOG_TYPE_ERROR);
		returnStatus = oam::API_FAILURE;
	}

	pthread_mutex_unlock(&THREAD_LOCK);
	return returnStatus;
}

/******************************************************************************************
* @brief	switchParentOAMModule
*
* purpose:	Switch OAM Parent Module
*
******************************************************************************************/
int ProcessManager::switchParentOAMModule(std::string newActiveModuleName)
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	int returnStatus = oam::API_SUCCESS;
	SNMPManager aManager;

	log.writeLog(__LINE__, "switchParentOAMModule Function Started", LOG_TYPE_DEBUG);

	// set alarm
	aManager.sendAlarmReport(newActiveModuleName.c_str(), MODULE_SWITCH_ACTIVE, SET);

	//set dbroots to read-write
	remountDbroots("ro");

	//clear run standby flag;
	runStandby = false;

	// update Calpont.xml entries
	string newActiveIPaddr;
	try
	{
		pthread_mutex_lock(&THREAD_LOCK);

		Config* sysConfig4 = Config::makeConfig();
	
		// get new Active address
		ModuleConfig moduleconfig;
		oam.getSystemConfig(newActiveModuleName, moduleconfig);
		HostConfigList::iterator pt2 = moduleconfig.hostConfigList.begin();
		newActiveIPaddr = (*pt2).IPAddr;

		sysConfig4->setConfig("ProcMgr", "IPAddr", newActiveIPaddr);
		sysConfig4->setConfig("ProcStatusControl", "IPAddr", newActiveIPaddr);
		sysConfig4->setConfig("DBRM_Controller", "IPAddr", newActiveIPaddr);

		// update Parent OAM Module name to current module name
		sysConfig4->setConfig("SystemConfig", "ParentOAMModuleName", newActiveModuleName);

		// clear Standby OAM Module
		sysConfig4->setConfig("SystemConfig", "StandbyOAMModuleName", oam::UnassignedName);
		sysConfig4->setConfig("ProcStatusControlStandby", "IPAddr", oam::UnassignedIpAddr);

		//update Calpont Config table
		try {
			sysConfig4->write();
		}
		catch(...)
		{
			log.writeLog(__LINE__, "ERROR: sysConfig->write", LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_FAILURE;
		}

		pthread_mutex_unlock(&THREAD_LOCK);

		//set DDL/DMLproc IPs to local module
		setPMProcIPs(newActiveModuleName);

		//distribute config file
		distributeConfigFile("system");	

		log.writeLog(__LINE__, "Calpont.xml entries update to local IP address of " + newActiveIPaddr, LOG_TYPE_DEBUG);
	}
	catch (exception& ex)
	{
		pthread_mutex_unlock(&THREAD_LOCK);
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
		return API_FAILURE;
	}
	catch(...)
	{
		pthread_mutex_unlock(&THREAD_LOCK);
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		return API_FAILURE;
	}

	//send message to local Process Monitor for OAM Cold Activation
	ByteStream msg1;
	ByteStream::byte requestID = OAMPARENTCOLD;

	msg1 << requestID;
	while(true)
	{
		int returnStatus = sendMsgProcMon( config.moduleName(), msg1, requestID );
	
		log.writeLog(__LINE__, "sent OAM Parent Cold message to local Process-Monitor, status: " + oam.itoa(returnStatus) , LOG_TYPE_DEBUG);
		if ( returnStatus == oam::API_SUCCESS)
			break;
	}

	//send message to new Active Process Monitor for OAM Parent Activation
	ByteStream msg;
	requestID = OAMPARENTACTIVE;

	msg << requestID;

	while(true)
	{
		int returnStatus = sendMsgProcMon( newActiveModuleName, msg, requestID );
	
		log.writeLog(__LINE__, "sent OAM Parent Activate message to New Active Process-Monitor, status: " + oam.itoa(returnStatus) , LOG_TYPE_DEBUG);
		if ( returnStatus == oam::API_SUCCESS)
			break;
	}

	// stop local SNMPTrapDaemon, if configured
	string EnableSNMP = "y";
	try {
		oam.getSystemConfig("EnableSNMP", EnableSNMP);
	}
	catch(...)
	{}

	if ( EnableSNMP == "y" )
		stopProcess(config.moduleName(), "SNMPTrapDaemon", oam::FORCEFUL, true);

	// start processmanager on new active node
	startProcess(newActiveModuleName, "ProcessManager", oam::FORCEFUL);

	// clear alarm
	aManager.sendAlarmReport(newActiveModuleName.c_str(), MODULE_SWITCH_ACTIVE, CLEAR);

	return returnStatus;
}

/******************************************************************************************
* @brief	OAMParentModuleChange
*
* purpose:	OAM Parent Module Change-over
*			The module will take over running as the OAM Parent module
*			after a detected outage
*
*
******************************************************************************************/
int ProcessManager::OAMParentModuleChange()
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;

	//
	//monitor OAM Parent module for outage
	//

	log.writeLog(__LINE__, "OAMParentModuleChange Function Started", LOG_TYPE_DEBUG);

	// Get Module Info
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

	string downOAMParentIPAddress;
	string downOAMParentName = config.OAMParentName();

	//Build module list
	vector<string>	moduleNameList;
	vector<string>	moduleIPAddrList;

	for ( unsigned int i = 0 ; i < systemModuleTypeConfig.moduletypeconfig.size(); i++)
	{
		int moduleCount = systemModuleTypeConfig.moduletypeconfig[i].ModuleCount;
		if ( moduleCount == 0 )
			// skip of no modules configured
			continue;

		DeviceNetworkList::iterator pt = systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
		for( ; pt != systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
		{
			HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

			//get parent module IP address
			if ( (*pt).DeviceName == downOAMParentName ) {
				downOAMParentIPAddress = (*pt1).IPAddr;
				continue;
			}

			//store the other modules
			if ( (*pt).DeviceName != config.moduleName() ) {
				moduleNameList.push_back((*pt).DeviceName);
				moduleIPAddrList.push_back((*pt1).IPAddr);
			}
		}
	}

	string HA_IPAddr;
	if ( moduleIPAddrList.empty() )
	{
		//get HA IP Address
		Config* sysConfig = Config::makeConfig();
		HA_IPAddr = sysConfig->getConfig("ProcMgr_HA", "IPAddr");
	
		log.writeLog(__LINE__, "Get HA_IPAddr = " + HA_IPAddr, LOG_TYPE_DEBUG);
		if ( !HA_IPAddr.empty() ) {
			moduleNameList.push_back("HA_device");
			moduleIPAddrList.push_back(HA_IPAddr);
		}
	}

	int ModuleHeartbeatCount;

	try {
		oam.getSystemConfig("ModuleHeartbeatCount", ModuleHeartbeatCount);
	}
	catch (exception& ex) {
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
	}

	string cmdLine = "ping ";
	string cmdOption = " -c 1 -w 5 >> /dev/null";
	string cmd;

	int pingFailure = 0;
	bool failover = false;
	bool recoveryTest = false;
	int disableCount = 0;
	int noAckCount = 0;

	while(!failover)
	{
		// check if a signal was received to start failover
		if (startFailOver) {
			//send notification going from standby to active
			oam.sendDeviceNotification(config.moduleName(), START_STANDBY_TO_MASTER);
			break;
		}

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
			log.writeLog(__LINE__, "Your trial period has expired.", LOG_TYPE_CRITICAL);
			//sleep until system gets stopped
			while(true) {
				sleep(100000);
			}
		}

		// perform ping test of Active Parent Module
		string cmd = cmdLine + downOAMParentIPAddress + cmdOption;
		int rtnCode = system(cmd.c_str());

		switch (rtnCode) {
			case 0:
			{
				//Ack ping
				pingFailure = 0;
				if ( noAckCount != 0 )
					oam.sendDeviceNotification(config.moduleName(), MODULE_UP);
				noAckCount = 0;
				sleep(1);
				break;
			}

			default:
			{
				//failed to respond to ping
				pingFailure++;
				log.writeLog(__LINE__, "OAMParentModule ping failure (" + downOAMParentName + ")", LOG_TYPE_WARNING);

				if ( pingFailure >= ModuleHeartbeatCount ) {

					bool ack = false;
					bool noack = false;

					//check NIC #1 status
					int sockfd;
					struct ifreq ifr;
				
					sockfd = socket(AF_INET, SOCK_DGRAM, 0);
					if(sockfd == -1){
						log.writeLog(__LINE__, "Could not get socket to check NIC #1", LOG_TYPE_ERROR);
						close(sockfd);
						break;
					}
				
					/* get interface name */
					strncpy(ifr.ifr_name, iface_name.c_str(), IFNAMSIZ);
				
					/* Read interface flags */
					if (ioctl(sockfd, SIOCGIFFLAGS, &ifr) < 0) {
						log.writeLog(__LINE__, "Error reading ioctl for NIC #1", LOG_TYPE_ERROR);
						close(sockfd);
						break;
					}
				
					if (ifr.ifr_flags & IFF_UP) {
						log.writeLog(__LINE__, "Local Interface is UP", LOG_TYPE_INFO);
						// any additional devices/modules to test
						if ( !moduleNameList.empty()) {
							// Active Parent not talking, check other modules or HA IP address
							for ( int count = 0 ; count <= ModuleHeartbeatCount ; count++ )
							{
								vector<string>::iterator pt1 = moduleNameList.begin();
								vector<string>::iterator pt2 = moduleIPAddrList.begin();
			
								for( ; pt1 != moduleNameList.end() ; pt1++, pt2++)
								{
									string cmd = cmdLine + *pt2 + cmdOption;
									int rtnCode = system(cmd.c_str());
							
									switch (rtnCode) {
										case 0:
										{ //Ack ping
											log.writeLog(__LINE__, *pt1 + " ping successful", LOG_TYPE_DEBUG);
											ack = true;
											break;
										}
				
										default:
										{ // ping failure	
											log.writeLog(__LINE__, *pt1 + " ping failure", LOG_TYPE_WARNING);
			
											noack = true;
											//save module name
											if ( *pt1 != "HA_device" )
												downModuleList.push_back(*pt1);
											break;
										}
									}
									// exit loop if ping was successfuly
									if ( ack )
										break;
		
									sleep (2);
								}
								// exit loop if ping was successfuly
								if ( ack )
									break;
							}
						}
						else
						{
							// NIC #1 up, procede with failover
							failover = true;
						}
					}
					else
					{
						log.writeLog(__LINE__, "NIC #1 is DOWN", LOG_TYPE_WARNING);
						// NIC #1 down, dont switch
						noack = true;
						if ( noAckCount == 0 )
							oam.sendDeviceNotification(config.moduleName(), MODULE_DOWN);
						noAckCount++;
					}

					close(sockfd);

					//check if all modules are not responding to ping
					if ( !ack && noack ) {
						// yes, go into hold state by setting local module to cold-state
						ByteStream msg;
						ByteStream::byte requestID = OAMPARENTCOLD;
					
						msg << requestID;
					
						int returnStatus = processManager.sendMsgProcMon( config.moduleName(), msg, requestID );	
						log.writeLog(__LINE__, "sent OAM Parent Cold message to local Process-Monitor, status: " + oam.itoa(returnStatus) , LOG_TYPE_DEBUG);
					}
					else
					{
						if ( ack && !noack ) {
							// all other modules ACK, only parent failed, procede with failover
							failover = true;
							break;
						}
						else
						{
							if ( ack && noack && !recoveryTest) {
							// some other modules ACK, some didn't
							// try 1 more time and mark sure didn't catch in the middle of a LAN recovery
								recoveryTest = true;
							}
							else
							{
								if ( ack && noack && recoveryTest) {
								// some other modules ACK, some didn't, partial outage, do failover
								failover = true;
								break;
								}
							}
						}
					}
				}
			}
		}

		if ( !failover ) {
			sleep(5);
			downModuleList.clear();
		}
		else
		{
			// PARENT PM OUTAGE DETECTED
			// check if disable flag is set, if so call the notification API
			string activePmFailoverDisabled;
			try {
				oam.getSystemConfig("ActivePmFailoverDisabled", activePmFailoverDisabled);

				if ( activePmFailoverDisabled == "y" ) {

					log.writeLog(__LINE__, "ActivePmFailoverDisabled is set, send notication", LOG_TYPE_DEBUG);

					oam.sendDeviceNotification(downOAMParentName, PM_MASTER_FAILED_DISABLED);
					failover = false;
					sleep(5);
					disableCount++;
					if ( disableCount > 4 ) {
						//no manually failover has been called, go ahead and do auto-failover
						//send notification going from standby to active

						log.writeLog(__LINE__, "ActivePmFailoverDisabled is set, but no manual action has been taken. Do Auto-Failover", LOG_TYPE_DEBUG);

						oam.sendDeviceNotification(config.moduleName(), START_STANDBY_TO_MASTER);
			
						break;
					}
				}
				else
				{
					// give a little time for the parent to shutdown and set the dbroot to read-only
					sleep (20);
		
					//send notification going from standby to active
					oam.sendDeviceNotification(config.moduleName(), START_STANDBY_TO_MASTER);
		
					break;
				}
			}
			catch (exception& ex)
			{}
			
		}
	}

	log.writeLog(__LINE__, " ", LOG_TYPE_DEBUG);
	log.writeLog(__LINE__, "*** OAMParentModule outage, OAM Parent Module change-over started ***", LOG_TYPE_DEBUG);

	//clear run standby flag;
	runStandby = false;

	//set dbroots to read-write
	remountDbroots("rw");

	//flush inode cache
	processManager.flushInodeCache();
	
	//run save.brm script
	processManager.saveBRM();

	gdownActiveOAMModule = downOAMParentName;

	// update Calpont.xml entries
	string localIPaddr;
	string newStandbyModule = downOAMParentName;
	string standbyIPaddr = downOAMParentIPAddress;
	try
	{
		pthread_mutex_lock(&THREAD_LOCK);

		Config* sysConfig4 = Config::makeConfig();
	
		// get my IP address
		ModuleConfig moduleconfig;
		oam.getSystemConfig(config.moduleName(), moduleconfig);
		HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
		localIPaddr = (*pt1).IPAddr;

		sysConfig4->setConfig("ProcMgr", "IPAddr", localIPaddr);
		sysConfig4->setConfig("ProcStatusControl", "IPAddr", localIPaddr);
		sysConfig4->setConfig("DBRM_Controller", "IPAddr", localIPaddr);

		// update Parent OAM Module name to current module name
		sysConfig4->setConfig("SystemConfig", "ParentOAMModuleName", config.moduleName());

		// clear Standby OAM Module
		sysConfig4->setConfig("SystemConfig", "StandbyOAMModuleName", oam::UnassignedName);
		sysConfig4->setConfig("ProcStatusControlStandby", "IPAddr", oam::UnassignedIpAddr);

		//update Calpont Config table
		try {
			sysConfig4->write();
		}
		catch(...)
		{
			log.writeLog(__LINE__, "ERROR: sysConfig->write", LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_FAILURE;
		}

		pthread_mutex_unlock(&THREAD_LOCK);

		//distribute config file
		distributeConfigFile("system");	

		//re-read config info again
		Configuration config;
		oam.setHotStandbyPM(standbyIPaddr);

		log.writeLog(__LINE__, "Calpont.xml Standby OAM updated : " + newStandbyModule + ":" + standbyIPaddr, LOG_TYPE_DEBUG);
		log.writeLog(__LINE__, "Calpont.xml entries update to local IP address of " + localIPaddr, LOG_TYPE_DEBUG);
	}
	catch (exception& ex)
	{
		pthread_mutex_unlock(&THREAD_LOCK);
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
		return API_FAILURE;
	}
	catch(...)
	{
		pthread_mutex_unlock(&THREAD_LOCK);
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		return API_FAILURE;
	}

	//set DDL/DMLproc IPs to local module
	setPMProcIPs(config.moduleName());

	//send message to local Process Monitor for OAM Parent Activation
	ByteStream msg;
	ByteStream::byte requestID = OAMPARENTACTIVE;

	msg << requestID;

	while(true)
	{
		int returnStatus = sendMsgProcMon( config.moduleName(), msg, requestID );
	
		log.writeLog(__LINE__, "sent OAM Parent Activate message to local Process-Monitor, status: " + oam.itoa(returnStatus) , LOG_TYPE_DEBUG);
		if ( returnStatus == oam::API_SUCCESS)
			break;
	}

	//set Process Manager state, will make sure process-monitor status control is working
	while (true)
	{
		try{
			ProcessStatus procstat;
			oam.getProcessStatus("ProcessManager", config.moduleName(), procstat);

			int ret = setProcessState(config.moduleName(), "ProcessManager", oam::ACTIVE, 0);
			if ( ret == oam::API_SUCCESS ) {
				oam.getProcessStatus("ProcessManager", config.moduleName(), procstat);
				if ( procstat.ProcessOpState == oam::ACTIVE )
					break;
			}
		}
		catch (...)
		{}
		sleep(1);
	}

	//set status to BUSY_INIT while failover is in progress
	processManager.setSystemState(oam::BUSY_INIT);

	// graceful start snmptrap-daemon, if configured
	string EnableSNMP = "y";
	try {
		oam.getSystemConfig("EnableSNMP", EnableSNMP);
	}
	catch(...)
	{}

	if ( EnableSNMP == "y" )
		startProcess(config.moduleName(), "SNMPTrapDaemon", oam::GRACEFUL);

	// set alarm
	SNMPManager aManager;
	aManager.sendAlarmReport(config.moduleName().c_str(), MODULE_SWITCH_ACTIVE, SET);

	//set down Active module to disable state
	disableModule(downOAMParentName, false);

	//do it here to get current processes active faster to process queries faster
	processManager.setProcessStates(downOAMParentName, oam::AUTO_OFFLINE);

	//set other down modules to disable state
	vector<string>::iterator pt1 = downModuleList.begin();

	for( ; pt1 != downModuleList.end() ; pt1++)
	{
		disableModule(*pt1, false);
		processManager.setProcessStates(*pt1, oam::AUTO_OFFLINE);
	}

	//distribute config file
	distributeConfigFile("system");	

	// stop local processes to restart after controller node and load_brm
	stopProcess(config.moduleName(), "DBRMWorkerNode", oam::FORCEFUL, true);
	stopProcess(config.moduleName(), "PrimProc", oam::FORCEFUL, true);
	stopProcess(config.moduleName(), "ExeMgr", oam::FORCEFUL, true);

	//reinit other DBRM worker nodes
//	reinitProcessType("DBRMWorkerNode");

	//restart mysqls
	processManager.restartProcessType("mysql");

	//send start module to local Process Monitor to startup any Cold-Standby processes
	string localModule = config.moduleName();
	processManager.setModuleState(localModule, oam::AUTO_INIT);
	pthread_t startmodulethread;
	int status = pthread_create (&startmodulethread, NULL, (void*(*)(void*)) &startModuleThread, &localModule);

	if ( status != 0 )
		log.writeLog(__LINE__, "startModuleThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

	//send message to start new Standby Process-Manager, if needed
	newStandbyModule = getStandbyModule();

	if ( !newStandbyModule.empty() && newStandbyModule != downOAMParentName 
			&& newStandbyModule != "NONE") {
		// get standby IP address and update entries
		setStandbyModule(newStandbyModule);
	}
/*	else
	{
		//setup downed module as standby-module
		setStandbyModule(downOAMParentName, false);
	}
*/
	//send message to each child process to start any COLD_STANDBY processes
	SystemModuleTypeConfig systemmoduletypeconfig;

	try{
		oam.getSystemConfig(systemmoduletypeconfig);
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

	for( unsigned int i = 0; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
	{
		int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
		if( moduleCount == 0)
			continue;

		DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
		for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
		{
			int opState = oam::ACTIVE;
			bool degraded;
			try {
				oam.getModuleStatus((*pt).DeviceName, opState, degraded);
			}
			catch (exception& ex)
			{
				string error = ex.what();
				log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": " + error, LOG_TYPE_ERROR);
			}
			catch(...)
			{
				log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": Caught unknown exception!", LOG_TYPE_ERROR);
			}

			if (opState == oam::ACTIVE) {
				if ((*pt).DeviceName != downOAMParentName ) {
					if ((*pt).DeviceName != config.moduleName() ) {
						processManager.setModuleState((*pt).DeviceName, oam::AUTO_INIT);
						pthread_t startmodulethread;
						string moduleName = (*pt).DeviceName;
						int status = pthread_create (&startmodulethread, NULL, (void*(*)(void*)) &startModuleThread, &moduleName);

						if ( status != 0 )
							log.writeLog(__LINE__, "startModuleThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

						sleep(1);
					}
				}
			}
		}
	}

	processManager.setSystemState(oam::ACTIVE);

	// clear alarm
	aManager.sendAlarmReport(config.moduleName().c_str(), MODULE_SWITCH_ACTIVE, CLEAR);

	log.writeLog(__LINE__, "*** Exiting OAMParentModuleChange function ***", LOG_TYPE_DEBUG);

	return API_SUCCESS;
}

/******************************************************************************************
* @brief	sendStatusUpdate
*
* purpose:	Send Status Update to Process Monitor
*
*
******************************************************************************************/
void ProcessManager::sendStatusUpdate(ByteStream obs, ByteStream::byte returnRequestType)
{
	try
	{
		MessageQueueClient processor("ProcStatusControl");
		processor.syncProto(false);
		ByteStream ibs;

		processor.write(obs);

		// wait 10 seconds for ACK from Process Monitor
		struct timespec ts = { 10, 0 };

		ibs = processor.read(&ts);

		if (ibs.length() > 0)
		{
			ByteStream::byte status;
			ibs >> status;
			if ( status == oam::API_SUCCESS ) {
				processor.shutdown();
			}
			else
			{
				// shutdown connection
				processor.shutdown();
				throw std::runtime_error("error");
			}
		}
		else
		{
			// timeout occurred, shutdown connection
			processor.shutdown();
			throw std::runtime_error("timeout");
		}
	}
	catch(...)
	{
		throw std::runtime_error("timeout");
	}

	Configuration config;
	Config* sysConfig5 = Config::makeConfig();

	if ( sysConfig5->getConfig("ProcStatusControlStandby", "IPAddr") == oam::UnassignedIpAddr )
		return;

	try
	{
		MessageQueueClient processor("ProcStatusControlStandby");
		processor.syncProto(false);
		ByteStream ibs;

		processor.write(obs);

		processor.shutdown();
	}
	catch(...)
	{}

	return;

}

/******************************************************************************************
* @brief	getStandbyModule
*
* purpose:	find an avaliable hot-standby module based on Process-Manager status, if one exist
*
*
******************************************************************************************/
std::string ProcessManager::getStandbyModule()
{
	Oam oam;
	SystemProcessStatus systemprocessstatus;
	ProcessStatus processstatus;
	string backupStandbyModule = "NONE";
	string newStandbyModule = "NONE";

	log.writeLog(__LINE__, "getStandbyModule called", LOG_TYPE_DEBUG);

	if ( gsharedNothingFlag) 
		return "";

	try
	{
		oam.getProcessStatus(systemprocessstatus);

		for( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
		{
			if ( systemprocessstatus.processstatus[i].ProcessName == "ProcessManager" &&
				systemprocessstatus.processstatus[i].ProcessOpState == oam::STANDBY )
				//already have a hot-standby
				return "";

			if ( backupStandbyModule != "NONE" )
				continue;

			if ( systemprocessstatus.processstatus[i].ProcessName == "ProcessManager" &&
				systemprocessstatus.processstatus[i].ProcessOpState == oam::COLD_STANDBY )
				// Found a ProcessManager in a COLD_STANDBY state
				newStandbyModule = systemprocessstatus.processstatus[i].Module;

			if ( systemprocessstatus.processstatus[i].ProcessName == "ProcessManager" &&
				systemprocessstatus.processstatus[i].ProcessOpState == oam::MAN_OFFLINE &&
				backupStandbyModule == "NONE")
				// Found a ProcessManager in a MAN_OFFLINE state, use if no COLD_STANDBY is found
				backupStandbyModule = systemprocessstatus.processstatus[i].Module;
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

	if ( newStandbyModule != "NONE" )
		return newStandbyModule;

	return backupStandbyModule;
}

/******************************************************************************************
* @brief	setStandbyModule
*
* purpose:	set Standby Module info in Calpont.xml
*
*
******************************************************************************************/
bool ProcessManager::setStandbyModule(std::string newStandbyModule, bool send)
{
	Oam oam;

	log.writeLog(__LINE__, "setStandbyModule called", LOG_TYPE_DEBUG);

	if ( newStandbyModule.empty() || gsharedNothingFlag)
		return true;

	pthread_mutex_lock(&THREAD_LOCK);

	for(int i=0 ; i < 5; i++)
	{
		// get standby IP address and update entries
		ModuleConfig moduleconfig;
		oam.getSystemConfig(newStandbyModule, moduleconfig);
		HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
		string standbyIPaddr = (*pt1).IPAddr;
	
		Configuration config;
		Config* sysConfig6 = Config::makeConfig();
		sysConfig6->setConfig("SystemConfig", "StandbyOAMModuleName", newStandbyModule);
		sysConfig6->setConfig("ProcStatusControlStandby", "IPAddr", standbyIPaddr);
	
		try{
			sysConfig6->write();
			pthread_mutex_unlock(&THREAD_LOCK);
	
			oam.setHotStandbyPM(standbyIPaddr);

			//distribute config file
			distributeConfigFile("system");	

			log.writeLog(__LINE__, "Calpont.xml Standby OAM updated to : " + newStandbyModule + ":" + standbyIPaddr, LOG_TYPE_DEBUG);

			if (send) {
				log.writeLog(__LINE__, "Send Message for new Hot-Standby ProcessManager to module = " + newStandbyModule, LOG_TYPE_DEBUG);
				int retStatus = startProcess(newStandbyModule, "ProcessManager", oam::GRACEFUL_STANDBY);
			
				log.writeLog(__LINE__, "Hot-Standby ProcessManager ACK received from Process-Monitor, return status = " + oam.itoa(retStatus), LOG_TYPE_DEBUG);
			}

			return true;
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "setStandbyModule: EXCEPTION ERROR on sysConfig->write(): " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "setStandbyModule :EXCEPTION ERROR on sysConfig->write(): Caught unknown exception!", LOG_TYPE_ERROR);
		}
	}

	log.writeLog(__LINE__, "setStandbyModule: failed to set enable state", LOG_TYPE_ERROR);

	pthread_mutex_unlock(&THREAD_LOCK);
	return false;

}

/******************************************************************************************
* @brief	clearStandbyModule
*
* purpose:	clear Standby Module info in Calpont.xml
*
*
******************************************************************************************/
bool ProcessManager::clearStandbyModule()
{
	Oam oam;

	log.writeLog(__LINE__, "clearStandbyModule called", LOG_TYPE_DEBUG);

	pthread_mutex_lock(&THREAD_LOCK);

	Configuration config;

	for(int i=0 ; i < 5; i++)
	{
		Config* sysConfig7 = Config::makeConfig();
		sysConfig7->setConfig("SystemConfig", "StandbyOAMModuleName", oam::UnassignedName);
		sysConfig7->setConfig("ProcStatusControlStandby", "IPAddr", oam::UnassignedIpAddr);
		try{
			sysConfig7->write();
			pthread_mutex_unlock(&THREAD_LOCK);
	
			oam.setHotStandbyPM(" ");
			log.writeLog(__LINE__, "Clear Calpont.xml Standby OAM", LOG_TYPE_DEBUG);

			//distribute config file
			distributeConfigFile("system");	

			return true;
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "clearStandbyModule: EXCEPTION ERROR on sysConfig->write(): " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "clearStandbyModule :EXCEPTION ERROR on sysConfig->write(): Caught unknown exception!", LOG_TYPE_ERROR);
		}
	
		sleep(1);
	}

	log.writeLog(__LINE__, "clearStandbyModule: failed to set enable state", LOG_TYPE_ERROR);

	pthread_mutex_unlock(&THREAD_LOCK);
	return false;

}

/******************************************************************************************
* @brief	setEnableState
*
* purpose:	clear Standby Module info in Calpont.xml
*
*
******************************************************************************************/
int ProcessManager::setEnableState(std::string target, std::string state)
{
	Oam oam;
	ModuleConfig moduleconfig;

	pthread_mutex_lock(&THREAD_LOCK);

	for(int i=0 ; i < 5; i++)
	{
		try
		{
			oam.getSystemConfig(target, moduleconfig);
	
			moduleconfig.DisableState = state;
	
			try
			{
				oam.setSystemConfig(target, moduleconfig);
				pthread_mutex_unlock(&THREAD_LOCK);
				return API_SUCCESS;
			}
			catch (exception& ex)
			{
				string error = ex.what();
				log.writeLog(__LINE__, "setEnableState: EXCEPTION ERROR on setSystemConfig: " + error, LOG_TYPE_ERROR);
			}
			catch(...)
			{
				log.writeLog(__LINE__, "setEnableState: EXCEPTION ERROR on setSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
			}
	
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "setEnableState: EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "setEnableState: EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		}

		sleep(1);
	}

	log.writeLog(__LINE__, "setEnableState: failed to set enable state", LOG_TYPE_ERROR);

	pthread_mutex_unlock(&THREAD_LOCK);
	return API_SUCCESS;

}

/******************************************************************************************
* @brief	remountDbroots
*
* purpose:	remount the dbroot disk, 'ro' or 'rw'
*
*
******************************************************************************************/
int ProcessManager::remountDbroots(std::string option)
{
	Oam oam;

	log.writeLog(__LINE__, "remountDbroots with option = " + option, LOG_TYPE_DEBUG);

	string DBRootStorageType;
	try {
		oam.getSystemConfig("DBRootStorageType", DBRootStorageType);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
		return oam::API_FAILURE;
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		return oam::API_FAILURE;
	}

	if ( DBRootStorageType != "storage" )
		return oam::API_SUCCESS;

	int DBRootCount;
	try {
		oam.getSystemConfig("DBRootCount", DBRootCount);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
		return oam::API_FAILURE;
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		return oam::API_FAILURE;
	}

	system("/usr/local/Calpont/bin/syslogSetup.sh uninstall  > /dev/null 2>&1");
	oam.syslogAction("sighup");

	for ( int i = 1 ; i < DBRootCount+1 ; i ++ )
	{
		string dbroot = "DBRoot" + oam.itoa(i);
		string dbrootDir;
		try {
			oam.getSystemConfig(dbroot, dbrootDir);
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
			system("/usr/local/Calpont/bin/syslogSetup.sh install  > /dev/null 2>&1");
			oam.syslogAction("sighup");
			return oam::API_FAILURE;
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
			system("/usr/local/Calpont/bin/syslogSetup.sh install  > /dev/null 2>&1");
			oam.syslogAction("sighup");
			return oam::API_FAILURE;
		}

		//unmount dbroot directory
		string cmd = "mount " + dbrootDir + " -o " + option + ",remount";
		system(cmd.c_str());
	}

	system("/usr/local/Calpont/bin/syslogSetup.sh install  > /dev/null 2>&1");
	oam.syslogAction("sighup");

	return oam::API_SUCCESS;
}

/******************************************************************************************
* @brief	flushInodeCache
*
* purpose:	flush cache during failover
*
*
******************************************************************************************/
void ProcessManager::flushInodeCache()
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

/******************************************************************************************
* @brief	sendUpgradeRequest
*
* purpose:	send Upgrade Request Msg to all ACTIVE UMs
*
*
******************************************************************************************/
void sendUpgradeRequest()
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	bool exitThread = false;
	int exitThreadStatus = oam::API_SUCCESS;

	// wait until DMLProc is ACTIVE
	while(true)
	{
		try{
			ProcessStatus procstat;
			oam.getProcessStatus("DMLProc", config.moduleName(), procstat);
			if ( procstat.ProcessOpState == oam::ACTIVE)
				break;
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

	SystemModuleTypeConfig systemmoduletypeconfig;

	try{
		oam.getSystemConfig(systemmoduletypeconfig);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
		exitThread = true;
		exitThreadStatus = oam::API_FAILURE;
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		exitThread = true;
		exitThreadStatus = oam::API_FAILURE;
	}

	if ( exitThread )
		pthread_exit((void*) exitThreadStatus);

	ByteStream msg;
	ByteStream::byte requestID = RUNUPGRADE;

	msg << requestID;
	msg << " ";		// pass a blank dummy password

	int returnStatus = oam::API_SUCCESS;

	for( unsigned int i = 0; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
	{
		int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
		if( moduleCount == 0)
			continue;

		string moduleType = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;
		if ( moduleType == "um" ||
			( moduleType == "pm" && config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM ) ) {

			DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
			{
				int opState;
				bool degraded;
				try {
					oam.getModuleStatus((*pt).DeviceName, opState, degraded);

					if (opState == oam::ACTIVE) {
						returnStatus = processManager.sendMsgProcMon( (*pt).DeviceName, msg, requestID, 30 );
		
						upgradethreadStatus = returnStatus;
		
						if ( returnStatus != API_SUCCESS)
							break;
					}
				}
				catch (exception& ex)
				{
					string error = ex.what();
					log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": " + error, LOG_TYPE_ERROR);
				}
				catch(...)
				{
					log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + (*pt).DeviceName + ": Caught unknown exception!", LOG_TYPE_ERROR);
				}
			}
		}
	}
	pthread_exit(0);
}

/******************************************************************************************
* @brief	stopProcessTypes
*
* purpose:	stop by process type
*
*
******************************************************************************************/
void ProcessManager::stopProcessTypes(bool manualFlag)
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;

	// skip if single server install, meaning only 1 worker node
	try {
		Config* sysConfig = Config::makeConfig();
		if ( sysConfig->getConfig("DBRM_Controller", "NumWorkers") == "1" )
			return;
	}
	catch(...)
	{
		return;
	}

	log.writeLog(__LINE__, "stopProcessTypes Called");

	processManager.stopProcessType("DMLProc", manualFlag);
	processManager.stopProcessType("DDLProc", manualFlag);

	processManager.stopProcessType("PrimProc", manualFlag);
	processManager.stopProcessType("ExeMgr", manualFlag);

	processManager.stopProcessType("DBRMControllerNode", manualFlag);
	processManager.stopProcessType("DBRMWorkerNode", manualFlag);

	log.writeLog(__LINE__, "stopProcessTypes Completed");
}


} //end of namespace

