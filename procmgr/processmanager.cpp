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

/******************************************************************************************
* $Id: processmanager.cpp 2216 2013-08-13 14:34:10Z dhill $
*
******************************************************************************************/

//#define NDEBUG
#include <cassert>

#include "processmanager.h"
#include "installdir.h"
#include "dbrm.h"
#include "cacheutils.h"
#include "ddlcleanuputil.h"
using namespace cacheutils;

using namespace std;
using namespace processmanager;
using namespace messageqcpp;
using namespace oam;
using namespace logging;
using namespace snmpmanager;
using namespace config;

pthread_mutex_t STATUS_LOCK;
pthread_mutex_t THREAD_LOCK;

extern string cloud;
extern bool amazon;
extern bool runStandby;
extern string iface_name;
extern string PMInstanceType;
extern string UMInstanceType;
extern string GlusterConfig;
extern bool rootUser;
extern string USER;
extern bool HDFS;
extern string localHostName;

typedef   map<string, int>	moduleList;
extern moduleList moduleInfoList;

bool gOAMParentModuleFlag;

oam::DeviceNetworkList startdevicenetworklist;

int upgradethreadStatus = oam::API_SUCCESS;
int startsystemthreadStatus = oam::API_SUCCESS;
int stopsystemthreadStatus = oam::API_SUCCESS;
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
	if (logType == LOG_TYPE_ERROR)
	{
		args.add("line:");
		args.add(lineNumber);
	}
	args.add(logContent);

	msg.format(args);

	switch(logType) {
		case LOG_TYPE_DEBUG:
			try {
				ml.logDebugMessage(msg);
			}
			catch(...) {}
			break;
		case LOG_TYPE_INFO:
			try {
				ml.logInfoMessage(msg);
			}
			catch(...) {}
			break;
		case LOG_TYPE_WARNING:
			try {
				ml.logWarningMessage(msg);
			}
			catch(...) {}
			break;
		case LOG_TYPE_ERROR:
			try {
				ml.logErrorMessage(msg);
			}
			catch(...) {}
			break;
		case LOG_TYPE_CRITICAL:
			try {
				ml.logCriticalMessage(msg);
			}
			catch(...) {}
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
void processMSG(messageqcpp::IOSocket* cfIos)
{
	messageqcpp::IOSocket fIos = *cfIos;

	pthread_t ThreadId;
	ThreadId = pthread_self();

	ByteStream msg;

	try{
		msg = fIos.read();
	}
	catch(...)
	{
		pthread_detach (ThreadId);
		pthread_exit(0);
	}

	if (msg.length() <= 0) {
		fIos.close();
		pthread_detach (ThreadId);
		pthread_exit(0);
	}

	ByteStream::byte msgType;
	msg >> msgType;

	Oam oam;
	ProcessLog log;
//	log.writeLog(__LINE__, "** processMSG msg type: " + oam.itoa(msgType), LOG_TYPE_DEBUG);

	Configuration config;
	ProcessManager processManager(config, log);

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
		
							if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED) {
								status = API_DISABLED;
								log.writeLog(__LINE__, "Stop Module requested Ignored on a Disabled " + moduleName);
							}
							else {
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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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
							processManager.shutdownModule(moduleName, graceful, manualFlag, 0);

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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

						log.writeLog(__LINE__, "SHUTDOWNMODULE: ACK back to sender, return status = " + oam.itoa(status));
					}

					break;
				}
				case STARTMODULE:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Start Module request" );

					startsystemthreadStop = false;

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
		
							//call dbrm control
							oam.dbrmctl("halt");
							log.writeLog(__LINE__, "'dbrmctl halt' done", LOG_TYPE_DEBUG);

							oam.dbrmctl("reload");
							log.writeLog(__LINE__, "'dbrmctl reload' done", LOG_TYPE_DEBUG);
						
							oam.dbrmctl("resume");
							log.writeLog(__LINE__, "'dbrmctl resume' done", LOG_TYPE_DEBUG);

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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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

					startsystemthreadStop = false;

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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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
									|| opState == oam::AUTO_DISABLED ) {

								oam.dbrmctl("halt");
								log.writeLog(__LINE__, "'dbrmctl halt' done", LOG_TYPE_DEBUG);

								status = processManager.disableModule(moduleName, true);
								log.writeLog(__LINE__, "Disable Module Completed on " + moduleName, LOG_TYPE_INFO);

								//call dbrm control
								oam.dbrmctl("reload");
								log.writeLog(__LINE__, "'dbrmctl reload' done", LOG_TYPE_DEBUG);
							
								// resume the dbrm
								oam.dbrmctl("resume");
								log.writeLog(__LINE__, "'dbrmctl resume' done", LOG_TYPE_DEBUG);

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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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
		
							if (opState == oam::MAN_DISABLED) {
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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

						log.writeLog(__LINE__, "ENABLEMODULE: ACK back to sender");
					}

					break;
				}

				case STOPSYSTEM:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Stop System request..." );

					// GRACEFUL_WAIT means that we are shutting down, but waiting for
					// all transactions to finish or rollback as commanded. This is only set if
					// there are, in fact, transactions active (or cpimport).
					if (graceful == GRACEFUL_WAIT)
					{
						ByteStream stillWorkingMsg;
						stillWorkingMsg << (ByteStream::byte) oam::ACK;
						stillWorkingMsg << actionType;
						stillWorkingMsg << target;
						stillWorkingMsg << (ByteStream::byte) API_STILL_WORKING;

						// This wait can take a while. We wait for table locks to release and open transactions to commit.
						if (oam.waitForSystem(STOPSYSTEM, fIos, stillWorkingMsg))
						{
							graceful = GRACEFUL;	// ProcMonitor doesn't know GRACEFUL_WAIT.
							// Send an ack back to say we're done waiting and are now shutting down.
							ackMsg << (ByteStream::byte) oam::ACK;
							ackMsg << actionType;
							ackMsg << target;
							ackMsg << (ByteStream::byte) API_TRANSACTIONS_COMPLETE;
							try {
								fIos.write(ackMsg);
							}
							catch(...) {}
							log.writeLog(__LINE__, "STOPSYSTEM: ACK transactions complete back to sender, return status = " + oam.itoa(API_TRANSACTIONS_COMPLETE));
						}
						else
						{
							// We've been cancelled.
							if (ackIndicator)
							{
								ackMsg << (ByteStream::byte) oam::ACK;
								ackMsg << actionType;
								ackMsg << target;
								ackMsg << (ByteStream::byte) API_CANCELLED;
								try {
									fIos.write(ackMsg);
								}
								catch(...) {}

								log.writeLog(__LINE__, "STOPSYSTEM: ACK back to sender (canceled)");
								break;
							}
						}
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
					processManager.setSystemState(oam::MAN_INIT);

					if (HDFS)
					{
						oam::DeviceNetworkList devicenetworklist;
						pthread_t stopsystemthread;
						status = pthread_create (&stopsystemthread, NULL, (void*(*)(void*)) &stopSystemThread, &devicenetworklist);
	
						if ( status != 0 ) {
							log.writeLog(__LINE__, "STOPSYSTEMS: pthread_create failed, return status = " + oam.itoa(status));
							status = API_FAILURE;
						}
	
						if (status == 0 && ackIndicator)
						{	
							ackMsg << (ByteStream::byte) oam::ACK;
							ackMsg << actionType;
							ackMsg << target;
							ackMsg << (ByteStream::byte) status;

							try {
								fIos.write(ackMsg);
							}
							catch(...) {}
	
							log.writeLog(__LINE__, "STOPSYSTEM: ACK back to sender");
						}
						break;
					}

					//call to update module status and send notification message
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

							processManager.stopModule((*pt).DeviceName, STATUS_UPDATE, manualFlag, 0);
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

//							int retStatus = processManager.stopModule((*pt).DeviceName, graceful, manualFlag, 0);
							processManager.stopModule((*pt).DeviceName, graceful, manualFlag, 0);

//							log.writeLog(__LINE__, "STOPSYSTEM: ACK received from Process-Monitor, return status = " + oam.itoa(status));
//							if (retStatus != API_SUCCESS)
//								status = retStatus;
						}
					}

					//wait until all child modules are offline or A FAILURE HAS OCCURRED
					bool failure = false;
					bool stopped = true;
					for ( int retry = 0 ; retry < 120 ; retry++ )
					{
						sleep(1);
						stopped = true;
						for ( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
						{
							int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
							if ( moduleCount == 0)
								continue;
		
							DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
							for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
							{
								string moduleName = (*pt).DeviceName;
		
								//skip OAM Parent module, do at the end
								if ( moduleName == config.moduleName() )
									continue;

								int opState;
								try
								{
									bool degraded;
									oam.getModuleStatus(moduleName, opState, degraded);
									if (opState == oam::FAILED) {
										failure = true;
										log.writeLog(__LINE__, "STOPSYSTEM: Failed, failure on module " + moduleName, LOG_TYPE_ERROR);
										break;
									}
	
									if (opState == oam::MAN_OFFLINE || 
										opState == oam::MAN_DISABLED || 
										opState == oam::AUTO_DISABLED )
										continue;
									stopped = false;
								} 
								catch (exception& ex)
								{
									string error = ex.what();
									log.writeLog(__LINE__, "EXCEPTION ERROR on : " + error, LOG_TYPE_ERROR);
								} 
								catch (...)
								{
									log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": Caught unknown exception!", LOG_TYPE_ERROR);
								}
							}
							if ( failure )
								break;
						}

						if ( failure)
							break;
						if ( stopped )
							break;
					}

					if ( failure )
					{
						processManager.setSystemState(oam::FAILED);
					}
					else
					{
						if ( !stopped)
						{
							//timeout waiting for system to stop, error out
							log.writeLog(__LINE__, "STOPSYSTEM: Failed, timeout waiting for module to stop", LOG_TYPE_ERROR);
							processManager.setSystemState(oam::FAILED);
						}
						else
						{
							//now stop local module
							processManager.stopModule(config.moduleName(), graceful, manualFlag );
	
							//run save.brm script
							processManager.saveBRM();
	
							log.writeLog(__LINE__, "Stop System Completed Success", LOG_TYPE_INFO);
	
							processManager.setSystemState(oam::MAN_OFFLINE);
						}
					}

					if (ackIndicator)
					{
					ackMsg.reset();
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) API_SUCCESS;
						fIos.write(ackMsg);

						log.writeLog(__LINE__, "STOPMODULE: ACK back to sender");
					}

					startsystemthreadStop = false;

					break;
				}
				case SHUTDOWNSYSTEM:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Shutdown System request..." );

					// GRACEFUL_WAIT means that we are shutting down, but waiting for
					// all transactions to finish or rollback as commanded. This is only set if
					// there are, in fact, transactions active (or cpimport).
/*					if (graceful == GRACEFUL_WAIT)
					{
						ByteStream stillWorkingMsg;
						stillWorkingMsg << (ByteStream::byte) oam::ACK;
						stillWorkingMsg << actionType;
						stillWorkingMsg << target;
						stillWorkingMsg << (ByteStream::byte) API_STILL_WORKING;

						// This wait can take a while. We wait for table locks to release and open transactions to commit.
						if (oam.waitForSystem(SHUTDOWNSYSTEM, fIos, stillWorkingMsg))
						{
							graceful = GRACEFUL;	// ProcMonitor doesn't know GRACEFUL_WAIT.
							// Send an ack back to say we're done waiting and are now shutting down.
							ackMsg << (ByteStream::byte) oam::ACK;
							ackMsg << actionType;
							ackMsg << target;
							ackMsg << (ByteStream::byte) API_TRANSACTIONS_COMPLETE;
							try {
								fIos.write(ackMsg);
							}
							catch(...) {}
							log.writeLog(__LINE__, "SHUTDOWNSYSTEM: ACK transactions complete back to sender, return status = " + oam.itoa(API_TRANSACTIONS_COMPLETE));
						}
						else
						{
							// We've been cancelled.
							if (ackIndicator)
							{
								ackMsg << (ByteStream::byte) oam::ACK;
								ackMsg << actionType;
								ackMsg << target;
								ackMsg << (ByteStream::byte) API_CANCELLED;
								try {
									fIos.write(ackMsg);
								}
								catch(...) {}

								log.writeLog(__LINE__, "SHUTDOWNSYSTEM: ACK back to sender (canceled)");
								break;
							}
						}
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

							processManager.stopModule((*pt).DeviceName, STATUS_UPDATE, manualFlag, 0);
						}
					}

					//stop by process type first, if system is ACTIVE
					if (systemstatus.SystemOpState == ACTIVE)
					{
						processManager.stopProcessTypes(manualFlag);
					}
*/

					int retStatus = oam::API_SUCCESS;

					if (HDFS)
					{
						if (ackIndicator)
						{
							ackMsg.reset();
							ackMsg << (ByteStream::byte) oam::ACK;
							ackMsg << actionType;
							ackMsg << target;
							ackMsg << (ByteStream::byte) status;
							try {
								fIos.write(ackMsg);
							}
							catch(...) {}
	
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
	
						//run save.brm script
						processManager.saveBRM();

						string cmd = "pdsh -a -x " + localHostName + " '/etc/init.d/infinidb stop' > /dev/null 2>&1";
						system(cmd.c_str());

						break;
					}
					else
					{
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
								{
									continue;
								}
	
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
	
								retStatus = processManager.shutdownModule((*pt).DeviceName, graceful, manualFlag, 0);
							}
						}
					}

					if (ackIndicator)
					{
						ackMsg.reset();
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) status;
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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

					//run save.brm script
					processManager.saveBRM();

					// now do local module
					processManager.shutdownModule(config.moduleName(), graceful, manualFlag);

					break;
				}
				case STARTSYSTEM:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Start System request...ackIndicator=" + oam.itoa(ackIndicator));

					startsystemthreadStop = false;

					// get system status and don't process if already in-progress
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
								try {
									fIos.write(ackMsg);
								}
								catch(...) {}
		
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
						status = stopsystemthreadStatus;

						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) status;
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

						log.writeLog(__LINE__, "STARTSYSTEM: ACK back to sender");
					}

					log.writeLog(__LINE__, "STARTSYSTEM: Start System Request Completed with status = " + oam.itoa(status));

					break;
				}
				case RESTARTSYSTEM:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Restart System request..." );

					startsystemthreadStop = false;

					// GRACEFUL_WAIT means that we are shutting down, but waiting for
					// all transactions to finish or rollback as commanded. This is only set if
					// there are, in fact, transactions active (or cpimport).
					if (graceful == GRACEFUL_WAIT)
					{
						ByteStream stillWorkingMsg;
						stillWorkingMsg << (ByteStream::byte) oam::ACK;
						stillWorkingMsg << actionType;
						stillWorkingMsg << target;
						stillWorkingMsg << (ByteStream::byte) API_STILL_WORKING;

						// This wait can take a while. We wait for table locks to release and open transactions to commit.
						if (oam.waitForSystem(RESTARTSYSTEM, fIos, stillWorkingMsg))
						{
							graceful = GRACEFUL;	// ProcMonitor doesn't know GRACEFUL_WAIT.
							// Send an ack back to say we're done waiting and are now shutting down.
							ackMsg << (ByteStream::byte) oam::ACK;
							ackMsg << actionType;
							ackMsg << target;
							ackMsg << (ByteStream::byte) API_TRANSACTIONS_COMPLETE;
							try {
								fIos.write(ackMsg);
							}
							catch(...) {}
							log.writeLog(__LINE__, "RESTARTSYSTEM: ACK transactions complete back to sender, return status = " + oam.itoa(API_TRANSACTIONS_COMPLETE));
						}
						else
						{
							// We've been cancelled.
							if (ackIndicator)
							{
								ackMsg << (ByteStream::byte) oam::ACK;
								ackMsg << actionType;
								ackMsg << target;
								ackMsg << (ByteStream::byte) API_CANCELLED;
								try {
									fIos.write(ackMsg);
								}
								catch(...) {}

								log.writeLog(__LINE__, "RESTARTSYSTEM: ACK back to sender (canceled)");
								break;
							}
						}
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
                            // BUG 4554 We don't need the join because calpont console is now looking for "Active"
                            // We need to return the ack right away to let console know we got the message.
//							pthread_join(startsystemthread, NULL);
//							status = startsystemthreadStatus;
						}

						log.writeLog(__LINE__, "RESTARTSYSTEM: Start System Request Completed", LOG_TYPE_INFO);
					}
	
					if (ackIndicator)
					{
						ackMsg.reset();
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) status;
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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
#if 0	// A RESTARTPROCESS message is about to arrive, so this is redundant.
					if( target.find("PrimProc") == 0) {

						//distribute config file
						processManager.distributeConfigFile("system");	

						processManager.reinitProcessType("WriteEngineServer");
						processManager.reinitProcessType("ExeMgr");
						processManager.reinitProcessType("DDLProc");
						processManager.reinitProcessType("DMLProc");
					}

					// if a WriteEngineServer was restarted, restart DDL/DMLProc
					if( target.find("WriteEngineServer") == 0) {

						processManager.reinitProcessType("DDLProc");
						processManager.reinitProcessType("DMLProc");
					}
#endif
					// if DDL or DMLProc, change IP Address
					if( target.find("DDLProc") == 0 || 
						target.find("DMLProc") == 0 ) {

						processManager.setPMProcIPs(moduleName, target);
					}

					if (ackIndicator)
					{
						ackMsg << (ByteStream::byte) oam::ACK;
						ackMsg << actionType;
						ackMsg << target;
						ackMsg << (ByteStream::byte) status;
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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

						processManager.reinitProcessType("WriteEngineServer");
						processManager.reinitProcessType("ExeMgr");
						processManager.reinitProcessType("DDLProc");
						processManager.reinitProcessType("DMLProc");
					}

					// if a WriteEngineServer was restarted, restart DDL/DMLProc
					if( target.find("WriteEngineServer") == 0) {

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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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
					try {
						fIos.write(ackMsg);
					}
					catch(...) {}

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
					try {
						fIos.write(ackMsg);
					}
					catch(...) {}

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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}

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

						processManager.reinitProcessType("WriteEngineServer");
						processManager.reinitProcessType("ExeMgr");
						processManager.reinitProcessType("DDLProc");
						processManager.reinitProcessType("DMLProc");
					}

					// if a WriteEngineServer was restarted, restart DDL/DMLProc
					if( target.find("WriteEngineServer") == 0) {

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

							processManager.reinitProcessType("WriteEngineServer");
							processManager.reinitProcessType("ExeMgr");
							processManager.reinitProcessType("DDLProc");
							processManager.reinitProcessType("DMLProc");
						}
					}

					// if a WriteEngineServer was restarted, restart DDL/DMLProc
					if( target.find("WriteEngineServer") == 0) {

						processManager.reinitProcessType("DDLProc");
						processManager.reinitProcessType("DMLProc");
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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}
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
						try {
							fIos.write(ackMsg);
						}
						catch(...) {}
					}

					log.writeLog(__LINE__, "Distribute Config File Completed " + target + "/" + file);
					break;	
				}
	
				case SWITCHOAMPARENT:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Switch OAM Parent to : " + target);
						// GRACEFUL_WAIT means that we are shutting down, but waiting for
					// all transactions to finish or rollback as commanded. This is only set if
					// there are, in fact, transactions active (or cpimport).
					if (graceful == GRACEFUL_WAIT)
					{
						ByteStream stillWorkingMsg;
						stillWorkingMsg << (ByteStream::byte) oam::ACK;
						stillWorkingMsg << actionType;
						stillWorkingMsg << target;
						stillWorkingMsg << (ByteStream::byte) API_STILL_WORKING;

						// This wait can take a while. We wait for table locks to release and open transactions to commit.
						if (oam.waitForSystem(RESTARTSYSTEM, fIos, stillWorkingMsg))
						{
							graceful = GRACEFUL;	// ProcMonitor doesn't know GRACEFUL_WAIT.
							// Send an ack back to say we're done waiting and are now shutting down.
							ackMsg << (ByteStream::byte) oam::ACK;
							ackMsg << actionType;
							ackMsg << target;
							ackMsg << (ByteStream::byte) API_TRANSACTIONS_COMPLETE;
							try {
								fIos.write(ackMsg);
							}
							catch(...) {}
							log.writeLog(__LINE__, "SWITCHOAMPARENT: ACK transactions complete back to sender, return status = " + oam.itoa(API_TRANSACTIONS_COMPLETE));
						}
						else
						{
							// We've been cancelled.
							if (ackIndicator)
							{
								ackMsg << (ByteStream::byte) oam::ACK;
								ackMsg << actionType;
								ackMsg << target;
								ackMsg << (ByteStream::byte) API_CANCELLED;
								try {
									fIos.write(ackMsg);
								}
								catch(...) {}

								log.writeLog(__LINE__, "SWITCHOAMPARENT: ACK back to sender (canceled)");
								break;
							}
						}
					}

					status = processManager.switchParentOAMModule(target);
	
					log.writeLog(__LINE__, "Switch OAM Parent Completed", LOG_TYPE_INFO );

					ackMsg << (ByteStream::byte) oam::ACK;
					ackMsg << actionType;
					ackMsg << target;
					ackMsg << (ByteStream::byte) status;
					try {
						fIos.write(ackMsg);
					}
					catch(...) {}

					// stop myself
					processManager.stopProcess(config.moduleName(), "ProcessManager", oam::FORCEFUL, true);

					break;	
				}

				case UNMOUNT:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Unmount dbroot : " + target);
	
					status = processManager.unmountDBRoot(target);
	
					log.writeLog(__LINE__, "UnMount Completed status: " + oam.itoa(status) );

					ackMsg << (ByteStream::byte) oam::ACK;
					ackMsg << actionType;
					ackMsg << target;
					ackMsg << (ByteStream::byte) status;
					try {
						fIos.write(ackMsg);
					}
					catch(...) {}

					break;	
				}

				case MOUNT:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: mount dbroot : " + target);
	
					status = processManager.mountDBRoot(target);
	
					log.writeLog(__LINE__, "Mount Completed status: " + oam.itoa(status) );

					ackMsg << (ByteStream::byte) oam::ACK;
					ackMsg << actionType;
					ackMsg << target;
					ackMsg << (ByteStream::byte) status;
					try {
						fIos.write(ackMsg);
					}
					catch(...) {}

					break;	
				}

				case SUSPENDWRITES:
				{
					ByteStream::byte ackResponse = API_FAILURE;
					log.writeLog(__LINE__,  "MSG RECEIVED: suspend database writes");

					// GRACEFUL_WAIT means that we are Suspending writes, but waiting for all
					// transactions to finish or rollback as commanded. This is only set if there
					// are, in fact, transactions active (or cpimport).
					if (graceful == GRACEFUL_WAIT)
					{
						ByteStream stillWorkingMsg;
						stillWorkingMsg << (ByteStream::byte) oam::ACK;
						stillWorkingMsg << actionType;
						stillWorkingMsg << target;
						stillWorkingMsg << (ByteStream::byte) API_STILL_WORKING;

						// This wait can take a while. We wait for table locks to release and open transactions to commit.
						if (oam.waitForSystem(SUSPENDWRITES, fIos, stillWorkingMsg))
						{
							graceful = GRACEFUL;	// ProcMonitor doesn't know GRACEFUL_WAIT.
							// Send an ack back to say we're done waiting and are now shutting down.
							ackMsg << (ByteStream::byte) oam::ACK;
							ackMsg << actionType;
							ackMsg << target;
							ackMsg << (ByteStream::byte) API_TRANSACTIONS_COMPLETE;
							try {
								fIos.write(ackMsg);
							}
							catch(...) {}
							log.writeLog(__LINE__, "SUSPENDWRITES: ACK transactions complete back to sender, return status = " + oam.itoa(API_TRANSACTIONS_COMPLETE));
						}
						else
						{
							// We've been cancelled.
							if (ackIndicator)
							{
								ackMsg << (ByteStream::byte) oam::ACK;
								ackMsg << actionType;
								ackMsg << target;
								ackMsg << (ByteStream::byte) API_CANCELLED;
								try {
									fIos.write(ackMsg);
								}
								catch(...) {}

								log.writeLog(__LINE__, "SUSPENDWRITES: ACK back to sender (canceled)");
								break;
							}
						}
					}

					BRM::DBRM dbrm;
					dbrm.setSystemSuspended(true);
					// Wait for everything to settle down
					sleep(5);
					// Save the BRM. This command presages a system backup. Best to have a current BRM on disk
					string logdir("/var/log/Calpont");
					if (access(logdir.c_str(), W_OK) != 0) logdir = "/tmp";
					string cmd = startup::StartUp::installDir() + "/bin/save_brm  > " + logdir + "/save_brm.log1 2>&1";
					int rtnCode = system(cmd.c_str());
					if (WEXITSTATUS(rtnCode) == 0)
					{
						ackResponse = API_SUCCESS;
					}
					else
					{
						ackResponse = API_FAILURE_DB_ERROR;
						dbrm.setSystemSuspended(false);
					}

					ackMsg.reset();
					ackMsg << (ByteStream::byte) oam::ACK;
					ackMsg << actionType;
					ackMsg << target;
					ackMsg << ackResponse;
					try {
						fIos.write(ackMsg);
					}
					catch(...) {}

					log.writeLog(__LINE__, "SUSPENDWRITES: ACK back to sender" + oam.itoa(ackResponse));
					break;
				}

				case FSTABUPDATE:
				{
					log.writeLog(__LINE__,  "MSG RECEIVED: Distribute Fstab update" );

					//get fstab entry
					string entry;
					msg >> entry;

					status = API_SUCCESS;
 
					if ( target == "system" )
					{
						//send out to all pms
						for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
						{
							if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType != "pm" )
								continue;
	
							int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
							if( moduleCount == 0)
								continue;
			
							DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
							for (;pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
							{
								int retStatus = processManager.updateFstab((*pt).DeviceName, entry);
								if (retStatus != API_SUCCESS)
									status = retStatus;
							}
						}
					}
					else
					{
						int retStatus = processManager.updateFstab(target, entry);
						if (retStatus != API_SUCCESS)
							status = retStatus;
					}

					ackMsg << (ByteStream::byte) oam::ACK;
					ackMsg << actionType;
					ackMsg << target;
					ackMsg << (ByteStream::byte) status;
					try {
						fIos.write(ackMsg);
					}
					catch(...) {}

					log.writeLog(__LINE__, "FSTABUPDATE: ACK back to sender, return status = " + oam.itoa(status));

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
					try {
						fIos.write(ackMsg);
					}
					catch(...) {}

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
			
								processManager.reinitProcessType("WriteEngineServer");
								processManager.reinitProcessType("ExeMgr");
								processManager.reinitProcessType("DDLProc");
								processManager.reinitProcessType("DMLProc");
							}

							// if a WriteEngineServer was restarted, restart DDL/DMLProc
							if( processName == "WriteEngineServer") {
		
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
								processManager.reinitProcessType("DMLProc");
							}

							//only run on auto process restart
							if (manual == 0 )
							{
								//get dbhealth flag
								string DBHealthMonitorFlag = "n";
								string DBFunctionalMonitorFlag;
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
										oam.checkDBFunctional();
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

				string moduleName;

				msg >> moduleName;

				int ret = processManager.getDBRMData(fIos, moduleName);

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
	
	sleep(5);
	fIos.close();
	pthread_detach (ThreadId);
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
			try {
				fIos.write(msg);
			}
			catch(...) {}
		
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
			try {
				fIos.write(msg);
			}
			catch(...) {}
		
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

	try {
		fIos.write(msg);
	}
	catch(...) {}

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
int ProcessManager::stopModule(string target, ByteStream::byte actionIndicator, bool manualFlag, int timeout)
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
	else
	{
		log.writeLog(__LINE__, target + " module is stopped by request.", LOG_TYPE_DEBUG);
	
		if ( manualFlag ) {
			setModuleState(target, oam::MAN_INIT);
		}
		else
		{
			setModuleState(target, oam::AUTO_INIT);
		}
	}

	returnStatus = sendMsgProcMon( target, msg, requestID, timeout );

	if ( actionIndicator != STATUS_UPDATE )
	{
		if ( returnStatus == API_SUCCESS)
		{
			//Issue an alarm, log the event 
			log.writeLog(__LINE__, target + " module is successfully stopped.", LOG_TYPE_DEBUG);
	
			if ( manualFlag ) {
//				setModuleState(target, oam::MAN_OFFLINE);
			
				//Issue an alarm 
				SNMPManager aManager;				
				aManager.sendAlarmReport(target.c_str(), MODULE_DOWN_MANUAL, SET);
			}
			else
			{
//				setModuleState(target, oam::AUTO_OFFLINE);
			
				//Issue an alarm 
				SNMPManager aManager;
				aManager.sendAlarmReport(target.c_str(), MODULE_DOWN_AUTO, SET);
			}
		}
		else
		{
//			if ( manualFlag ) {
//				setModuleState(target, oam::FAILED);
//			}
	
			//log the event 
			log.writeLog(__LINE__, target + " module failed to stop!!", LOG_TYPE_WARNING);
		}
	}

	return returnStatus;
}

/******************************************************************************************
* @brief	shutdownModule
*
* purpose:	power off the specified module,
*
******************************************************************************************/
int ProcessManager::shutdownModule(string target, ByteStream::byte actionIndicator, bool manualFlag, int timeout)
{
	ByteStream msg;
	ByteStream::byte requestID = SHUTDOWNMODULE;
	string processName = "";

	msg = buildRequestMessage(requestID, actionIndicator, processName, manualFlag);

	int returnStatus = sendMsgProcMon( target, msg, requestID, timeout );

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
	
	setModuleState(target, newState);

	//set Calpont.xml enbale state
	setEnableState( target, SnewState);

	log.writeLog(__LINE__, "disableModule - setEnableState", LOG_TYPE_DEBUG);

	//sleep a bit to give time for the state change to apply
	sleep(1);

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

	log.writeLog(__LINE__, "disableModule successfully complete for " + target, LOG_TYPE_DEBUG);

	return API_SUCCESS;
}

/******************************************************************************************
* @brief	recycleProcess
*
* purpose:	recyle process, general;ly after some disable module is run
*
******************************************************************************************/
void ProcessManager::recycleProcess(string module)
{
	Oam oam;
	ModuleConfig moduleconfig;

	log.writeLog(__LINE__, "recycleProcess request after module was disabled: " + module, LOG_TYPE_DEBUG);

	string moduleType = module.substr(0,MAX_MODULE_TYPE_SIZE);

	// if a UM module send a restart on DMLProc/DDLProc to get started on another UM, if needed
	string PrimaryUMModuleName;
	try {
		oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);
	}
	catch(...) {}

	//restart ExeMgrs/mysql if module is a pm
	if ( moduleType == "pm" ) {
		restartProcessType("ExeMgr");
		restartProcessType("mysql");
	}
	else
		reinitProcessType("ExeMgr");

	if ( PrimaryUMModuleName == module )
	{
		restartProcessType("DDLProc", false);
		sleep(1);
		restartProcessType("DMLProc", false);
	}

	if( moduleType == "pm" && PrimaryUMModuleName != module)
	{
		reinitProcessType("DDLProc");
		sleep(1);
		restartProcessType("DMLProc", false);
	}

	return;
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

	if (setEnableState( target, oam::ENABLEDSTATE) != API_SUCCESS )
		return API_FAILURE;

	setModuleState(target, state);

	//sleep a bit to give time for the state change to apply
	sleep(5);

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
				|| ( (*itor).ModuleType == "ChildOAMModule")
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
	messageqcpp::ByteStream::byte actionIndicator, bool manualFlag, int timeout)
{
	ByteStream msg;
	ByteStream::byte requestID = STOP;
	
	msg = buildRequestMessage(requestID, actionIndicator, processName, manualFlag);

	int returnStatus = sendMsgProcMon( moduleName, msg, requestID, timeout );

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

	int returnStatus = sendMsgProcMon( moduleName, msg, requestID, 0 );

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

	int setState = state;

	// if system state = ACTIVE, make sure DMLProc is ACTIVE first
	if( state == oam::ACTIVE ) {
		setState = -1;
		// default to loal module
		string PrimaryUMModuleName = config.moduleName();
		try {
			oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);
		}
		catch(...) {}

		int retry = 0;
        while (retry < 30)
        {
			ProcessStatus DMLprocessstatus;
			try {
				oam.getProcessStatus("DMLProc", PrimaryUMModuleName, DMLprocessstatus);
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

			if (DMLprocessstatus.ProcessOpState == oam::FAILED) {
				setState = oam::FAILED;
				break;
			}

			if (DMLprocessstatus.ProcessOpState == oam::ACTIVE) {
				setState = oam::ACTIVE;
				break;
			}

            sleep(2);
			retry++;
        }
	}

	if ( setState != -1 )
	{
		pthread_mutex_lock(&STATUS_LOCK);
		try{
			oam.setSystemStatus(setState);
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
		if( setState == oam::ACTIVE ) {
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
int ProcessManager::setProcessState(string moduleName, string processName, uint16_t state, pid_t PID)
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
			|| ( (*itor).ModuleType == "ChildOAMModule" )
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

	int returnStatus = sendMsgProcMon( moduleName, msg, requestID, 30 );

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

	int returnStatus = sendMsgProcMon( moduleName, msg, requestID, 30 );

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

	int returnStatus = sendMsgProcMon( moduleName, msg, requestID, 30 );

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
* @brief	updateFstab
*
* purpose:	send Fstab Update to a specific module
*
******************************************************************************************/
int ProcessManager::updateFstab(std::string moduleName, std::string entry)
{
	ByteStream msg;
	ByteStream::byte requestID = PROCFSTABUPDATE;

	msg << requestID;
	msg << entry;

	int returnStatus = sendMsgProcMon( moduleName, msg, requestID, 30 );

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
				processManager.stopProcess(systemprocessstatus.processstatus[i].Module, 
																processName, 
																GRACEFUL, 
																manualFlag, 0);
//				log.writeLog(__LINE__, "stopProcessType: Start ACK received from Process-Monitor, return status = " + oam.itoa(retStatus), LOG_TYPE_DEBUG);
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
int ProcessManager::restartProcessType( std::string processName, bool manualFlag )
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
				if ( systemprocessstatus.processstatus[i].ProcessName == processName ) {
					//skip if in a BUSY_INIT state
					if ( systemprocessstatus.processstatus[i].ProcessOpState == oam::BUSY_INIT )
						continue;
					if ( (systemprocessstatus.processstatus[i].ProcessOpState == oam::ACTIVE) ||
							(systemprocessstatus.processstatus[i].ProcessOpState == oam::AUTO_OFFLINE) ||
							(systemprocessstatus.processstatus[i].ProcessOpState == oam::COLD_STANDBY && !manualFlag) ) {
						if( processName.find("DDLProc") == 0 || 
							processName.find("DMLProc") == 0 ) {
	
							try {
								oam.setSystemConfig("PrimaryUMModuleName", systemprocessstatus.processstatus[i].Module);

								processManager.setPMProcIPs(systemprocessstatus.processstatus[i].Module);

								//distribute config file
								processManager.distributeConfigFile("system");
								sleep(1);
							}
							catch(...) {}
						}

						// found one, request restart of it
						retStatus = processManager.restartProcess(systemprocessstatus.processstatus[i].Module, 
																		processName, 
																		FORCEFUL, 
																		true);
						log.writeLog(__LINE__, "restartProcessType: Start ACK received from Process-Monitor, return status = " + oam.itoa(retStatus), LOG_TYPE_DEBUG);

						// if DDL or DMLProc, change IP Address
						if ( retStatus == oam::API_SUCCESS )
						{
							if( processName.find("DDLProc") == 0 || 
								processName.find("DMLProc") == 0 ) {
		
								processManager.setPMProcIPs(systemprocessstatus.processstatus[i].Module, processName);
								return retStatus;
							}
						}
					}
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
		// re-init cpimport on all nodes
		if ( processName == "cpimport" ) {
			for( unsigned int i = 0 ; i < systemprocessstatus.processstatus.size(); i++)
			{
				if ( systemprocessstatus.processstatus[i].ProcessName == "ServerMonitor" ) {
					// found one, request reinit of it
					retStatus = processManager.reinitProcess(systemprocessstatus.processstatus[i].Module, 
																	"cpimport");
					log.writeLog(__LINE__, "reinitProcessType: ACK received from Process-Monitor, return status = " + oam.itoa(retStatus), LOG_TYPE_DEBUG);
				}
			}
		}
		else
		{
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
int ProcessManager::addModule(oam::DeviceNetworkList devicenetworklist, std::string password, bool manualFlag)
{
	SystemModuleTypeConfig systemmoduletypeconfig;
	ModuleTypeConfig moduletypeconfig;
	ModuleTypeConfig setmoduletypeconfig;
	DeviceNetworkConfig devicenetworkconfig;
	Oam oam;
	string Section;
	string installDir = startup::StartUp::installDir();

	pthread_mutex_lock(&THREAD_LOCK);

	int AddModuleCount = devicenetworklist.size();
	DeviceNetworkList::iterator listPT = devicenetworklist.begin();
	string moduleType = (*listPT).DeviceName.substr(0,MAX_MODULE_TYPE_SIZE);

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
						if ( hostname == oam::UnassignedName )
							continue;

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
	string calpontPackage1;
	string calpontPackage2;

	string systemID;
	string packageType = "rpm";

	oam.getSystemConfig("EEPackageType", packageType);

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
	string homedir = "/root";
	if (!rootUser) {
		char* p= getenv("HOME");
		if (p && *p)
			homedir = p;
	}

	if ( packageType != "binary") {
		string separator = "-";
		if ( packageType == "deb" )
			separator = "_";
		calpontPackage = homedir + "/infinidb-platform" + separator + systemsoftware.Version + "-" + systemsoftware.Release + "*." + packageType;
		mysqlPackage = homedir + "/infinidb-storage-engine" + separator + systemsoftware.Version + "-" + systemsoftware.Release + "*." + packageType;
		mysqldPackage = homedir + "/infinidb-mysql" + separator + systemsoftware.Version + "-" + systemsoftware.Release + "*." + packageType;
		calpontPackage1 = homedir + "/infinidb-libs" + separator + systemsoftware.Version + "-" + systemsoftware.Release + "*." + packageType;
		calpontPackage2 = homedir + "/infinidb-ent" + separator + systemsoftware.Version + "-" + systemsoftware.Release + "*." + packageType;
	}
	else
	{
		calpontPackage = homedir + "/infinidb*" + systemsoftware.Version + "-" + systemsoftware.Release + "*.bin.tar.gz";
		mysqlPackage = calpontPackage;
		mysqldPackage = calpontPackage;
	}

	string cmd = "ls " + calpontPackage + " > /dev/null 2>&1";
	int rtnCode = system(cmd.c_str());
	if (WEXITSTATUS(rtnCode) != 0) {
		log.writeLog(__LINE__, "addModule - ERROR: Package not found: " + calpontPackage, LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_FILE_OPEN_ERROR;
	}
	log.writeLog(__LINE__, "addModule - Calpont Package found:" + calpontPackage, LOG_TYPE_DEBUG);

	// check if infinidb-ent is there
	cmd = "ls " + calpontPackage2 + " > /dev/null 2>&1";
	rtnCode = system(cmd.c_str());
	if (WEXITSTATUS(rtnCode) != 0)
		calpontPackage2 = "dummy.rpm";

	//
	// Verify Host IP and Password
	//

	// This is the password that is set in a amazon AMI
	string amazonDefaultPassword = "Calpont1";

	if ( password == "ssh" && amazon )
	{	// check if there is a root password stored
		string rpw = oam::UnassignedName;
		try
		{
			oam.getSystemConfig("rpw", rpw);
		}
		catch(...)
		{
			rpw = oam::UnassignedName;
		}

		if (rpw != oam::UnassignedName)
			password = rpw;
	}

	listPT = devicenetworklist.begin();
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
		string newHostName = (*pt1).HostName;
		if ( newHostName == oam::UnassignedName )
			continue;

		string newIPAddr = (*pt1).IPAddr;
		string cmd = installDir + "/bin/remote_command.sh " + newIPAddr + " " + password + " ls";
		log.writeLog(__LINE__, cmd, LOG_TYPE_DEBUG);
		int rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) != 0) {
			log.writeLog(__LINE__, "addModule - ERROR: Remote login test failed, Invalid IP / Password " + newIPAddr, LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_FAILURE;
		}
		log.writeLog(__LINE__, "addModule - Remote login test successful: " + newIPAddr, LOG_TYPE_DEBUG);
	}

	//
	//Get System Configuration file
	//

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
		string moduleName = (*listPT).DeviceName;
		devicenetworkconfig.DeviceName = (*listPT).DeviceName;
		devicenetworkconfig.DisableState = oam::MANDISABLEDSTATE;

		HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
		for( ; pt1 != (*listPT).hostConfigList.end() ; pt1++)
		{
			string hostName = (*pt1).HostName;
			string IPAddr = (*pt1).IPAddr;
			//if cloud and unassigned, launch a new Instance
			if ( ( cloud == "amazon-ec2" && hostName == oam::UnassignedName ) ||
				 ( cloud == "amazon-vpc" && hostName == oam::UnassignedName ) )
			{
				string UMinstanceType;
				string UMSecurityGroup;
				if ( moduleType == "um")
				{
					try{
						oam.getSystemConfig("UMInstanceType", UMinstanceType);
						oam.getSystemConfig("UMSecurityGroup", UMSecurityGroup);
					}
					catch(...) {}
				}	

				log.writeLog(__LINE__, "addModule - Launching a new Instance for: " + moduleName, LOG_TYPE_DEBUG);

				if ( moduleType == "um" )
					hostName = oam.launchEC2Instance(moduleName, IPAddr, UMinstanceType, UMSecurityGroup);
				else
					hostName = oam.launchEC2Instance(moduleName, IPAddr);

				if ( hostName == "failed" ) {
					log.writeLog(__LINE__, "addModule - Launch New Instance Failure", LOG_TYPE_ERROR);
					pthread_mutex_unlock(&THREAD_LOCK);
					return API_FAILURE;
				}

				//wait until login is success until continuing or fail if can't login
				log.writeLog(__LINE__, "addModule - Successfully Launch of new Instance, retry login test: " + moduleName, LOG_TYPE_DEBUG);
				int retry = 0;
				for (  ; retry < 60 ; retry++)
				{
					IPAddr = oam.getEC2InstanceIpAddress(hostName);
					if (IPAddr == "terminated") {
						log.writeLog(__LINE__, "addModule - Failed to log in to Instance, it was terminated: " + hostName, LOG_TYPE_ERROR);
						pthread_mutex_unlock(&THREAD_LOCK);
						return API_FAILURE;
					}

					if (IPAddr == "stopped") {
						sleep(10);
						continue;
					}

					string cmd = installDir + "/bin/remote_command.sh " + IPAddr + " " + amazonDefaultPassword + " 'ls' 1  > /tmp/login_test.log";
					system(cmd.c_str());
					if (!oam.checkLogStatus("/tmp/login_test.log", "README")) {
						//check for RSA KEY ISSUE and fix
						if (oam.checkLogStatus("/tmp/login_test.log", "Offending RSA key")) {
							log.writeLog(__LINE__, "addModule - login failed, RSA key issue, try fixing: " + moduleName, LOG_TYPE_DEBUG);
							string file = "/tmp/login_test.log";
							oam.fixRSAkey(file);
						}

						log.writeLog(__LINE__, "addModule - login failed, retry login test: " + moduleName, LOG_TYPE_DEBUG);
						sleep(10);
						continue;
					}

					// logged in
					break;
				}

				if ( retry >= 60 )
				{
					log.writeLog(__LINE__, "addModule - Failed to log in to Instance: " + hostName, LOG_TYPE_ERROR);
					pthread_mutex_unlock(&THREAD_LOCK);
					return API_FAILURE;
				}

				log.writeLog(__LINE__, "addModule - Successful loggin: " + hostName, LOG_TYPE_DEBUG);

				// add instance tag
				string systemName;
				string AmazonAutoTagging;
				{
					try{
						oam.getSystemConfig("SystemName", systemName);
						oam.getSystemConfig("AmazonAutoTagging", AmazonAutoTagging);
					}
					catch(...) {}
				}

				if ( AmazonAutoTagging == "y" )
				{
					string tagValue = systemName + "-" + moduleName;
					oam.createEC2tag( hostName, "Name", tagValue );
				}

				log.writeLog(__LINE__, "addModule - Launched new Instance: " + hostName + "/" + IPAddr, LOG_TYPE_DEBUG);

				(*pt1).HostName = hostName;
				(*pt1).IPAddr = IPAddr;

				//check if any volumes need to be attached
				if ( moduleType == "um" )
				{
					string UMStorageType = "internal";
					{
						try{
							oam.getSystemConfig("UMStorageType", UMStorageType);
						}
						catch(...) {}
					}

					if ( UMStorageType == "external" )
					{	//check if volume already assigned or need to create a new one
						int moduleID = atoi((*listPT).DeviceName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());

						string volumeNameID = "UMVolumeName" + oam.itoa(moduleID);
						string volumeName = oam::UnassignedName;
						string deviceNameID = "UMVolumeDeviceName" + oam.itoa(moduleID);
						string deviceName = oam::UnassignedName;
						try {
							oam.getSystemConfig( volumeNameID, volumeName);
							oam.getSystemConfig( deviceNameID, deviceName);
						}
						catch(...)
						{}
					
						if ( volumeName.empty() || volumeName == oam::UnassignedName ) {
							// need to create a new one
							string UMVolumeSize = "10";
							try{
								oam.getSystemConfig("UMVolumeSize", UMVolumeSize);
							}
							catch(...) {}

							log.writeLog(__LINE__, "addModule - Create new Volume for: " + (*listPT).DeviceName, LOG_TYPE_DEBUG);
							string volumeName = oam.createEC2Volume(UMVolumeSize);
							if ( volumeName == "failed" ) {
								log.writeLog(__LINE__, "addModule: create volume failed", LOG_TYPE_CRITICAL);
								pthread_mutex_unlock(&THREAD_LOCK);
								return API_FAILURE;
							}

							//attach and format volumes
							string device = "/dev/sdf" + oam.itoa(moduleID);

							string localInstance = oam.getEC2LocalInstance();

							//attach volumes to local instance
							log.writeLog(__LINE__, "addModule - Attach new Volume to local instance: " + volumeName, LOG_TYPE_DEBUG);
							if (!oam.attachEC2Volume(volumeName, device, localInstance)) {
								log.writeLog(__LINE__, "addModule: volume failed to attach to local instance", LOG_TYPE_CRITICAL);
								pthread_mutex_unlock(&THREAD_LOCK);
								return API_FAILURE;
							}
				
							//format attached volume
							log.writeLog(__LINE__, "addModule - Format new Volume for: " + volumeName, LOG_TYPE_DEBUG);
							string cmd = "mkfs.ext2 -F " + device + " > /dev/null 2>&1";
							system(cmd.c_str());
				
							//detach volume
							log.writeLog(__LINE__, "addModule - detach new Volume from local instance: " + volumeName, LOG_TYPE_DEBUG);
							if (!oam.detachEC2Volume(volumeName)) {
								log.writeLog(__LINE__, "addModule: volume failed to deattach to local instance", LOG_TYPE_CRITICAL);
								pthread_mutex_unlock(&THREAD_LOCK);
							}
			
							//attach to UM
							log.writeLog(__LINE__, "addModule - attach new Volume to " + moduleName, LOG_TYPE_DEBUG);
							if (!oam.attachEC2Volume(volumeName, device, hostName)) {
								log.writeLog(__LINE__, "addModule: volume failed to attach to um: " + moduleName, LOG_TYPE_CRITICAL);
								pthread_mutex_unlock(&THREAD_LOCK);
							}

							try {
								Config* sysConfig = Config::makeConfig();

								sysConfig->setConfig("Installation", volumeNameID, volumeName);
								sysConfig->setConfig("Installation", deviceNameID, device);

								sysConfig->write();
							}
							catch(...)
							{}

							// add instance tag
							string systemName;
							{
								try{
									oam.getSystemConfig("SystemName", systemName);
								}
								catch(...) {}
							}

							if ( AmazonAutoTagging == "y" )
							{
								string tagValue = systemName + "-" + moduleName;
								oam.createEC2tag( volumeName, "Name", tagValue );
							}

							log.writeLog(__LINE__, "addModule - create/attach new volume: " + volumeName + "/" + device, LOG_TYPE_DEBUG);

						}
						else
						{ // one exist, detach and reattach it

							oam.detachEC2Volume( volumeName );
					
							if (!oam.attachEC2Volume(volumeName, deviceName, hostName)) {
								log.writeLog(__LINE__, "addModule: volume failed to attached: " + volumeName, LOG_TYPE_CRITICAL);
								pthread_mutex_unlock(&THREAD_LOCK);
								return API_FAILURE;
							}

							log.writeLog(__LINE__, "addModule - attach existing volume: " + volumeName + "/" + deviceName, LOG_TYPE_DEBUG);
						}
					}
				}
			}

			hostconfig.HostName = hostName;
			hostconfig.IPAddr = IPAddr;
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
		sysConfig->setConfig(Section, "Port", "8800");

		Section = (*listPT).DeviceName + "_ServerMonitor";
		sysConfig->setConfig(Section, "IPAddr", (*pt1).IPAddr);
		sysConfig->setConfig(Section, "Port", "8622");
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

	if ( moduleType == "pm" ) {
		listPT = devicenetworklist.begin();
		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			Section = (*listPT).DeviceName + "_WriteEngineServer";
	
			HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
			sysConfig->setConfig(Section, "IPAddr", (*pt1).IPAddr);
			sysConfig->setConfig(Section, "Port", "8630");
		}
	}
	log.writeLog(__LINE__, "addModule - Updated Process Ports", LOG_TYPE_DEBUG);

	string parentOAMModuleHostName;
	string parentOAMModuleIPAddr;

	//setup dbroot entries
	if (moduleType == "pm" && manualFlag)
	{
        const string MODULE_DBROOTID = "ModuleDBRootID";
        const string MODULE_DBROOT_COUNT = "ModuleDBRootCount";

		listPT = devicenetworklist.begin();
		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			string moduleID = (*listPT).DeviceName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);

			string ModuleDBRootCount = MODULE_DBROOT_COUNT + moduleID + "-3";
			sysConfig->setConfig("SystemModuleConfig", ModuleDBRootCount, "0");

			string ModuleDBrootID = MODULE_DBROOTID + moduleID + "-1-3";
			sysConfig->setConfig("SystemModuleConfig", ModuleDBrootID, oam::UnassignedName);
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

	//check if any added modules are Active OAM
	bool activeOAM = false;
	listPT = devicenetworklist.begin();
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		if ( (*listPT).DeviceName == config.OAMParentName() ) {
			activeOAM = true;
			break;
		}
	}

	//
	//send message to Process Monitor to add module/processes to shared memory
	//
	if ( !activeOAM )
	{
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

		//create and copy custom OS
		//run remote installer script
		string dir = installDir + "/local/etc/" + remoteModuleName;

		string cmd = "mkdir " + dir + " > /dev/null 2>&1";
		system(cmd.c_str());

		if ( remoteModuleType == "um" ) {
			cmd = "cp " + installDir + "/local/etc/um1/* " + dir + "/.";
			system(cmd.c_str());
		}
		else
		{
			if ( remoteModuleType == "pm") {
				cmd = "cp " + installDir + "/local/etc/pm1/* " + dir + "/.";
				system(cmd.c_str());
			}
		}
		log.writeLog(__LINE__, "addModule - created directory and custom OS files for " +  remoteModuleName, LOG_TYPE_DEBUG);

		//create module file 
		if( !createModuleFile(remoteModuleName) ) {
			log.writeLog(__LINE__, "addModule - ERROR: createModuleFile failed", LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_FAILURE;
		}
		log.writeLog(__LINE__, "addModule - create module file for " +  remoteModuleName, LOG_TYPE_DEBUG);

		if ( remoteModuleType == "pm" ) {
			//setup Standby OAM Parent, if needed
			if ( config.OAMStandbyName() == oam::UnassignedName )
				setStandbyModule(remoteModuleName, false);
		}

		//set root password
		if (amazon) {
			cmd = startup::StartUp::installDir() + "/bin/remote_command.sh " + remoteModuleIP + " " + amazonDefaultPassword + " '/root/updatePassword.sh " + password + "' > /tmp/password_change.log";
			//log.writeLog(__LINE__, "addModule - cmd: " + cmd, LOG_TYPE_DEBUG);
			rtnCode = system(cmd.c_str());
			if (WEXITSTATUS(rtnCode) == 0)
				log.writeLog(__LINE__, "addModule - update root password: " + remoteModuleName, LOG_TYPE_DEBUG);
			else
				log.writeLog(__LINE__, "addModule - ERROR: update root password: " + remoteModuleName, LOG_TYPE_DEBUG);
		}

		//default
		string binaryInstallDir = installDir;

		//run installer on remote module
		if ( remoteModuleType == "um" ||
			( remoteModuleType == "pm" && config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM ) ) {
			//run remote installer script
			if ( packageType != "binary" ) {
				log.writeLog(__LINE__, "addModule - user_installer run for " +  remoteModuleName, LOG_TYPE_DEBUG);
				string cmd = installDir + "/bin/user_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage + " " + calpontPackage1 + " " + calpontPackage2 + " " + mysqlPackage + " " + mysqldPackage + " initial " + packageType +
				" --nodeps none 1 > /tmp/user_installer.log";
				log.writeLog(__LINE__, "addModule cmd: " + cmd, LOG_TYPE_DEBUG);

				rtnCode = system(cmd.c_str());
				if (WEXITSTATUS(rtnCode) != 0) {
					log.writeLog(__LINE__, "addModule - ERROR: user_installer.sh failed", LOG_TYPE_ERROR);
					pthread_mutex_unlock(&THREAD_LOCK);
					system(" cp /tmp/user_installer.log /tmp/user_installer.log.failed");
					return API_FAILURE;
				}
			}
			else
			{	// do a binary package install
				log.writeLog(__LINE__, "addModule - binary_installer run for " +  remoteModuleName, LOG_TYPE_DEBUG);

				string cmd = installDir + "/bin/binary_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage + " " + remoteModuleType + " initial " + oam.itoa(config.ServerInstallType()) + " 1 " + binaryInstallDir + " > /tmp/binary_installer.log";

				log.writeLog(__LINE__, "addModule - " + cmd, LOG_TYPE_DEBUG);
				rtnCode = system(cmd.c_str());
				if (WEXITSTATUS(rtnCode) != 0) {
					log.writeLog(__LINE__, "addModule - ERROR: binary_installer.sh failed", LOG_TYPE_ERROR);
					system(" cp /tmp/binary_installer.log /tmp/binary_installer.log.failed");
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
					string cmd = installDir + "/bin/performance_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage + " " + calpontPackage1 + " " + calpontPackage2 + " " + mysqlPackage + " " + mysqldPackage + " initial " + 	packageType + " --nodeps 1 > /tmp/performance_installer.log";
					log.writeLog(__LINE__, "addModule cmd: " + cmd, LOG_TYPE_DEBUG);

					rtnCode = system(cmd.c_str());
					if (WEXITSTATUS(rtnCode) != 0) {
						log.writeLog(__LINE__, "addModule - ERROR: performance_installer.sh failed", LOG_TYPE_ERROR);
						system(" cp /tmp/performance_installer.log /tmp/performance_installer.log.failed");
						pthread_mutex_unlock(&THREAD_LOCK);
						return API_FAILURE;
					}
				}
				else
				{	// do a binary package install
					log.writeLog(__LINE__, "addModule - binary_installer run for " +  remoteModuleName, LOG_TYPE_DEBUG);

					string cmd = installDir + "/bin/binary_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage + " " + remoteModuleType + " initial " + oam.itoa(config.ServerInstallType()) + " 1 " + binaryInstallDir + " > /tmp/binary_installer.log";
					log.writeLog(__LINE__, "addModule - " + cmd, LOG_TYPE_DEBUG);

					rtnCode = system(cmd.c_str());
					if (WEXITSTATUS(rtnCode) != 0) {
						log.writeLog(__LINE__, "addModule - ERROR: binary_installer.sh failed", LOG_TYPE_ERROR);
						system(" cp /tmp/binary_installer.log /tmp/binary_installer.log.failed");
						pthread_mutex_unlock(&THREAD_LOCK);
						return API_FAILURE;
					}
				}
			}
		}
	}

	//Start new modules by starting up local Process-Monitor
	listPT = devicenetworklist.begin();
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		string remoteModuleName = (*listPT).DeviceName;

		if (manualFlag)
			//set new module to disable state if manual add
			disableModule(remoteModuleName, true);

		HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
		string remoteModuleIP = (*pt1).IPAddr;
		string remoteHostName = (*pt1).HostName;

		//send start service commands
		string cmd = installDir + "/bin/remote_command.sh " + remoteModuleIP + " " + password + " '" + installDir + "/bin/infinidb restart;" + installDir + "/mysql/mysqld-Calpont restart' 0";
		system(cmd.c_str());
		log.writeLog(__LINE__, "addModule - restart infinidb service " +  remoteModuleName, LOG_TYPE_DEBUG);

		// add to monitor list
		moduleInfoList.insert(moduleList::value_type(remoteModuleName, 0));
		if (amazon) {
			//check and assign Elastic IP Address
			int AmazonElasticIPCount = 0;
			try{
				oam.getSystemConfig("AmazonElasticIPCount", AmazonElasticIPCount);
			}
			catch(...) {
				AmazonElasticIPCount = 0;
			}
	
			for ( int id = 1 ; id < AmazonElasticIPCount+1 ; id++ )
			{
				string AmazonElasticModule = "AmazonElasticModule" + oam.itoa(id);
				string ELmoduleName;
				try{
					oam.getSystemConfig(AmazonElasticModule, ELmoduleName);
				}
				catch(...) {}
	
				if ( ELmoduleName == remoteModuleName )
				{	//match found assign Elastic IP Address
					string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);
					string ELIPaddress;
					try{
						oam.getSystemConfig(AmazonElasticIPAddr, ELIPaddress);
					}
					catch(...) {}
	
					try{
						oam.assignElasticIP(remoteHostName, ELIPaddress);
						log.writeLog(__LINE__, "addModule - Set Elastic IP Address: " + remoteModuleName + "/" + ELIPaddress, LOG_TYPE_DEBUG);
					}
					catch(...) {
						log.writeLog(__LINE__, "addModule - Failed to Set Elastic IP Address: " + remoteModuleName + "/" + ELIPaddress, LOG_TYPE_ERROR);
					}
					break;
				}
			}
		}
	}

	//if amazon, delay to give time for ProcMon to start
	if (amazon) {
		log.writeLog(__LINE__, "addModule - sleep 30 - give ProcMon time to start on new Instance", LOG_TYPE_DEBUG);
		sleep(30);
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
int ProcessManager::removeModule(oam::DeviceNetworkList devicenetworklist, bool manualFlag)
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

	//validate the module list to be removed
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		int returnStatus = oam.validateModule((*listPT).DeviceName);
		if (returnStatus != API_SUCCESS) {
			log.writeLog(__LINE__, "removeModule - ERROR: invalid module: " + (*listPT).DeviceName, LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_INVALID_PARAMETER;
		}
	}

	if(manualFlag)
	{
		//stopModules being removed with the REMOVE option, which will stop process
		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			string moduleName = (*listPT).DeviceName;
			log.writeLog(__LINE__, "removeModule - stopping module: " + moduleName, LOG_TYPE_DEBUG);

			//don't allow remove of Active PM Module
			if ( moduleName == config.OAMParentName() ) {
				log.writeLog(__LINE__, "removeModule - ERROR: can't remove current module (Active Parent OAM) ", LOG_TYPE_ERROR);
				pthread_mutex_unlock(&THREAD_LOCK);
				return API_INVALID_PARAMETER;
			}

			int status;
			status = stopModule(moduleName, REMOVE, true);
	
			if (status == API_SUCCESS) {
				log.writeLog(__LINE__, "removeModule - stopModule Successfully " + moduleName, LOG_TYPE_DEBUG);
				//check for SIMPLEX Processes on mate might need to be started
				pthread_mutex_unlock(&THREAD_LOCK);
				checkSimplexModule(moduleName);
				pthread_mutex_lock(&THREAD_LOCK);
			}
			else
				log.writeLog(__LINE__, "removeModule - stopModule " + moduleName, LOG_TYPE_ERROR);
		}
	}

	int newModuleCount = oldModuleCount - RemoveModuleCount;
	setmoduletypeconfig.ModuleCount = newModuleCount;

	string systemName;
	string AmazonAutoTagging;
	{
		try{
			oam.getSystemConfig("SystemName", systemName);
			oam.getSystemConfig("AmazonAutoTagging", AmazonAutoTagging);
		}
		catch(...) {}
	}

	//Clear out Module IP and Hostnames
	listPT = devicenetworklist.begin();
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		string moduleName = (*listPT).DeviceName;
		log.writeLog(__LINE__, "removeModule - removing module: " + moduleName, LOG_TYPE_DEBUG);

		//don't allow remove of Active PM Module
		if ( moduleName == config.OAMParentName() ) {
			log.writeLog(__LINE__, "removeModule - ERROR: can't remove current module (Active Parent OAM) ", LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_INVALID_PARAMETER;
		}

		DeviceNetworkList::iterator pt = setmoduletypeconfig.ModuleNetworkList.begin();
		for ( ; pt != setmoduletypeconfig.ModuleNetworkList.end() ; pt++)
		{
			if ( moduleName == (*pt).DeviceName ) {
				HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
				for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++ )
				{
					//if cloud, delete instance
					if (amazon)
					{
						log.writeLog(__LINE__, "removeModule - terminate instance: " + (*pt1).HostName, LOG_TYPE_DEBUG);
						oam.terminateEC2Instance( (*pt1).HostName );

						// update instance tag
						if ( AmazonAutoTagging == "y" )
						{
							string tagValue = systemName + "-" + moduleName + "-terminated";
							oam.createEC2tag( (*pt1).HostName, "Name", tagValue );
						}
					}

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

	//unassign dbroot entries
	if (moduleType == "pm")
	{
        const string MODULE_DBROOTID = "ModuleDBRootID";
        const string MODULE_DBROOT_COUNT = "ModuleDBRootCount";

		listPT = devicenetworklist.begin();
		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			string moduleID = (*listPT).DeviceName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);

			string ModuleDBRootCount = MODULE_DBROOT_COUNT + moduleID + "-3";
			sysConfig->setConfig("SystemModuleConfig", ModuleDBRootCount, oam::UnassignedName);

			string ModuleDBrootID = MODULE_DBROOTID + moduleID + "-1-3";
			sysConfig->setConfig("SystemModuleConfig", ModuleDBrootID, oam::UnassignedName);
		}
	}

	log.writeLog(__LINE__, "removeModule - Updated DBRoot paramaters", LOG_TYPE_DEBUG);

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

	//check if any removed modules was Standby OAM or Active OAM
	bool activeOAM = false;
	listPT = devicenetworklist.begin();
	for( ; listPT != devicenetworklist.end() ; listPT++)
	{
		if ( (*listPT).DeviceName == config.OAMStandbyName() )
			clearStandbyModule();
		else
			if ( (*listPT).DeviceName == config.OAMParentName() )
				activeOAM = true;
	}

	//
	//send message to Process Monitor to remove module/processes to shared memory
	//
	if ( !activeOAM )
	{
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
		log.writeLog(__LINE__, "removeModule - successfully removed module: " + (*listPT).DeviceName, LOG_TYPE_DEBUG);
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
		log.writeLog(__LINE__, "reconfigureModule - stopModule Successfully " + moduleName, LOG_TYPE_DEBUG);
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
	string logdir("/var/log/Calpont");
	if (access(logdir.c_str(), W_OK) != 0) logdir = "/tmp";

	log.writeLog(__LINE__, "Running reset_locks", LOG_TYPE_DEBUG);

	string cmd = startup::StartUp::installDir() + "/bin/reset_locks > " + logdir + "/reset_locks.log1 2>&1";
	int rtnCode = system(cmd.c_str());
	if (WEXITSTATUS(rtnCode) != 1) {
		log.writeLog(__LINE__, "Successfully ran reset_locks", LOG_TYPE_DEBUG);
	}
	else
		log.writeLog(__LINE__, "Error running reset_locks", LOG_TYPE_ERROR);

	log.writeLog(__LINE__, "Running DBRM save_brm", LOG_TYPE_DEBUG);

	cmd = startup::StartUp::installDir() + "/bin/save_brm > " + logdir + "/save_brm.log1 2>&1";
	rtnCode = system(cmd.c_str());
	if (WEXITSTATUS(rtnCode) != 1) {
		log.writeLog(__LINE__, "Successfully ran DBRM save_brm", LOG_TYPE_DEBUG);
	}
	else
		log.writeLog(__LINE__, "Error running DBRM save_brm", LOG_TYPE_ERROR);
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

	string fileName = startup::StartUp::installDir() + "/local/etc/" + remoteModuleName + "/module";

	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());

	string cmd = "echo " + remoteModuleName + " > " + fileName;
	system(cmd.c_str());

	newFile.close();

	return true;
}


/*****************************************************************************************
* @brief	startSystemThread
*
* purpose:	Send Messages to Module Process Monitors to start Processes
*
*****************************************************************************************/
void startSystemThread(oam::DeviceNetworkList Devicenetworklist)
{
	oam::DeviceNetworkList devicenetworklist = Devicenetworklist;

	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	SystemModuleTypeConfig systemmoduletypeconfig;
	SNMPManager aManager;
	int status = API_SUCCESS;
	bool exitThread = false;
	int exitThreadStatus = oam::API_SUCCESS;

	pthread_t ThreadId;
	ThreadId = pthread_self();

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

	if ( exitThread ) {
		pthread_detach (ThreadId);
		pthread_exit(reinterpret_cast<void*>(static_cast<ptrdiff_t>(exitThreadStatus)));
	}

	if (systemstatus.SystemOpState == AUTO_OFFLINE)
		processManager.setSystemState(oam::AUTO_INIT);
	else
		processManager.setSystemState(oam::MAN_INIT);

	//validate the dbroots assignments
	//make sure no 1 ID is assigned to 2 PMs
	//and a dbroot not assigned to a DISABLED PM
	try
	{
		systemStorageInfo_t t;
		t = oam.getStorageConfig();

		DeviceDBRootList moduledbrootlist1 = boost::get<2>(t);
		DeviceDBRootList moduledbrootlist2 = boost::get<2>(t);

		DeviceDBRootList::iterator pt1 = moduledbrootlist1.begin();
		for( ; pt1 != moduledbrootlist1.end() ; pt1++)
		{
			string moduleID1 = oam.itoa((*pt1).DeviceID);
			string moduleName = "pm" + moduleID1;

			// check DISABLED modules
			int opState;
			bool degraded;
			try{
				oam.getModuleStatus(moduleName, opState, degraded);
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

			//check if disabled
			if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED) {
				if ( (*pt1).dbrootConfigList.size() != 0 ) {
					//issue log and Set the alarm 
					log.writeLog(__LINE__, "startSystemThread failed: Disabled Module '" + moduleName + "' has  DBRoots assigned to it", LOG_TYPE_CRITICAL);
					aManager.sendAlarmReport(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, SET);
					startsystemthreadStatus = oam::API_FAILURE;
					processManager.setSystemState(oam::FAILED);
					pthread_detach (ThreadId);
					pthread_exit((void*) oam::API_FAILURE);
				}

				continue;
			}

			// if module has no dbroots assigned, fail startSystem
			if ( (*pt1).dbrootConfigList.size() == 0 ) {
				//issue log and Set the alarm 
				log.writeLog(__LINE__, "startSystemThread failed: Module '" + moduleName + "' has no DBRoots assigned to it", LOG_TYPE_CRITICAL);
				aManager.sendAlarmReport(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, SET);
				startsystemthreadStatus = oam::API_FAILURE;
				processManager.setSystemState(oam::FAILED);
				pthread_detach (ThreadId);
				pthread_exit((void*) oam::API_FAILURE);
			}

			DBRootConfigList::iterator pt1a = (*pt1).dbrootConfigList.begin();
			for( ; pt1a != (*pt1).dbrootConfigList.end() ; pt1a++)
			{
				DeviceDBRootList::iterator pt2 = moduledbrootlist2.begin();
				for( ; pt2 != moduledbrootlist2.end() ; pt2++)
				{
					string moduleID2 = oam.itoa((*pt2).DeviceID);
					if ( moduleID1 == moduleID2 )
						continue;

					DBRootConfigList::iterator pt2a = (*pt2).dbrootConfigList.begin();
					for( ; pt2a != (*pt2).dbrootConfigList.end() ; pt2a++)
					{
						if ( *pt1a == *pt2a) {
							log.writeLog(__LINE__, "ERROR: DBRoot ID " + oam.itoa(*pt1a) + " configured on 2 pms: 'pm" + moduleID1 + "' and 'pm" + moduleID2 + "'", LOG_TYPE_CRITICAL);
							//Set the alarm 
							aManager.sendAlarmReport(config.moduleName().c_str(), STARTUP_DIAGNOTICS_FAILURE, SET);
					
							startsystemthreadStatus = oam::API_FAILURE;
							processManager.setSystemState(oam::FAILED);
							pthread_detach (ThreadId);
							pthread_exit((void*) oam::API_FAILURE);
						}
					}
				}
			}
		}
	}
	catch (exception& e)
	{} 

	try{
		oam.getSystemConfig(systemmoduletypeconfig);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
		startsystemthreadStatus = oam::API_FAILURE;
		processManager.setSystemState(oam::FAILED);
		exitThread = true;
		exitThreadStatus = oam::API_FAILURE;
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		startsystemthreadStatus = oam::API_FAILURE;
		processManager.setSystemState(oam::FAILED);
		exitThread = true;
		exitThreadStatus = oam::API_FAILURE;
	}

	if ( exitThread ) {
		pthread_detach (ThreadId);
		pthread_exit(reinterpret_cast<void*>(static_cast<ptrdiff_t>(exitThreadStatus)));
	}

	if (systemstatus.SystemOpState == AUTO_OFFLINE)
		processManager.setSystemState(oam::AUTO_INIT);
	else
		processManager.setSystemState(oam::MAN_INIT);

	startsystemthreadRunning = true;

	string newStandbyModule = processManager.getStandbyModule();

	if ( !newStandbyModule.empty() && newStandbyModule != "NONE")
		processManager.setStandbyModule(newStandbyModule);

	//update workernode section
	processManager.updateWorkerNodeconfig();

	//configure PMS ports
	if ( processManager.updatePMSconfig() != API_SUCCESS ) {
		startsystemthreadStatus = oam::API_FAILURE;
		processManager.setSystemState(oam::FAILED);
		pthread_detach (ThreadId);
		pthread_exit((void*) oam::API_FAILURE);
	}

	if ( devicenetworklist.size() != 0 ) {
		//distribute config file
		processManager.distributeConfigFile("system");

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

			pthread_t startmodulethread;
			int status = pthread_create (&startmodulethread, NULL, (void*(*)(void*)) &startModuleThread, &moduleName);
			
			if ( status != 0 )
				log.writeLog(__LINE__, "startModuleThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

			sleep(5);
		}
	}
	else {
		// start all modules, like on a systemStart command
		//launch start module threads, starting with local module

		if ( config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM )
		{
			try {
				oam.setSystemConfig("PrimaryUMModuleName", config.OAMParentName());
			}
			catch(...) {}

			processManager.setPMProcIPs(config.OAMParentName());
		}

		//distribute config file
		processManager.distributeConfigFile("system");

		pthread_t startmodulethread;
		string moduleName = config.moduleName();
		int status = pthread_create (&startmodulethread, NULL, (void*(*)(void*)) &startModuleThread, &moduleName);

		if ( status != 0 )
			log.writeLog(__LINE__, "startModuleThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

		sleep(5);

		for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
		{
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

				//setup primary User Module, DML/DDL only start on this module
				if ( moduleName.find("um") == 0 && config.ServerInstallType() != oam::INSTALL_COMBINE_DM_UM_PM)
				{
					string PrimaryUMModuleName;
					try {
						oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);
					}
					catch(...) {}

					if ( PrimaryUMModuleName == oam::UnassignedName )
					{
						try {
							oam.setSystemConfig("PrimaryUMModuleName", moduleName);
						}
						catch(...) {}

						processManager.setPMProcIPs(moduleName);

						//distribute config file
						processManager.distributeConfigFile("system");
					}
				}

				pthread_t startmodulethread;
				string name = moduleName;
				int status = pthread_create (&startmodulethread, NULL, (void*(*)(void*)) &startModuleThread, &name);

				if ( status != 0 )
					log.writeLog(__LINE__, "startModuleThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

				if ( !HDFS )
					sleep(5);
				else
					//usleep(100000);
					sleep(1);
			}
		}
	}

	// check status and process accordingly
	int k = 0;
	for( ; k < 1200 ; k++ )
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
			pthread_detach (ThreadId);
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
		
			if ( !newStandbyModule.empty() && newStandbyModule != "NONE") {
				// get standby IP address and update entries
				processManager.setStandbyModule(newStandbyModule);

				//distribute config file
				processManager.distributeConfigFile("system");
			}
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
	
	if ( k == 1200 ) {
		// system didn't Successfully restart
		log.writeLog(__LINE__, "startSystemThread: Modules failed to start after 1200 tries, Set System State to FAILED" , LOG_TYPE_CRITICAL);
		processManager.setSystemState(oam::FAILED);
		status = oam::API_FAILURE;
	}

    // Bug 4554: Wait until DMLProc is finished with rollback
    if (status == oam::API_SUCCESS)
    {
        BRM::DBRM dbrm;
        uint16_t rtn = 0;
        bool bfirst = true;
        SystemProcessStatus systemprocessstatus;

		string PrimaryUMModuleName;
		try {
			oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);
		}
		catch(...) {}

		if ( PrimaryUMModuleName.empty() )
		{
			log.writeLog(__LINE__, "startSystemThread: Failed, PrimaryUMModuleName is unassigned", LOG_TYPE_CRITICAL);
			rtn = oam::FAILED;
			log.writeLog(__LINE__, "startSystemThread Exit", LOG_TYPE_DEBUG);
			processManager.setSystemState(oam::FAILED);
			startsystemthreadStatus = status;
			startsystemthreadRunning = false;
			pthread_detach (ThreadId);
			pthread_exit(0);
		}

		// waiting until dml are ACTIVE, then mark system ACTIVE
        while (rtn == 0)
        {
			ProcessStatus DMLprocessstatus;
			try {
				oam.getProcessStatus("DMLProc", PrimaryUMModuleName, DMLprocessstatus);
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

			if (DMLprocessstatus.ProcessOpState == oam::BUSY_INIT) {
				if (bfirst)
				{
					log.writeLog(__LINE__, "Waiting for DMLProc to finish rollback" , LOG_TYPE_INFO);
					bfirst = false;
				}
			}

			if (DMLprocessstatus.ProcessOpState == oam::ACTIVE) {
                rtn = oam::ACTIVE;
				break;
			}

			if (DMLprocessstatus.ProcessOpState == oam::FAILED) {
                rtn = oam::FAILED;
				break;
			}

			// wait some more
            sleep(2);
        }
        processManager.setSystemState(rtn);
	}

	// exit thread
	log.writeLog(__LINE__, "startSystemThread Exit", LOG_TYPE_DEBUG);
	startsystemthreadStatus = status;
	startsystemthreadRunning = false;
	pthread_detach (ThreadId);
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

	//store in a local variable
	string moduleName = module;

	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	bool exitThread = false;
	int exitThreadStatus = oam::API_SUCCESS;

	pthread_t ThreadId;
	ThreadId = pthread_self();

	if ( moduleName.empty() ){
		log.writeLog(__LINE__, "startModuleThread received on invalid module name", LOG_TYPE_ERROR);
		pthread_detach (ThreadId);
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

		if ( exitThread ) {
			pthread_detach (ThreadId);
			pthread_exit(reinterpret_cast<void*>(static_cast<ptrdiff_t>(exitThreadStatus)));
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

		if ( startsystemthreadStop) {
			// set status and exit this thread
			processManager.setModuleState(moduleName, oam::MAN_OFFLINE);
			log.writeLog(__LINE__, "startModuleThread early exit on " + moduleName, LOG_TYPE_DEBUG);
			pthread_detach (ThreadId);
			pthread_exit(0);
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
	pthread_detach (ThreadId);
	pthread_exit(0);
}


/*****************************************************************************************
* @brief	stopSystemThread
*
* purpose:	Send Messages to Module Process Monitors to stop Processes
*
*****************************************************************************************/
void stopSystemThread(oam::DeviceNetworkList Devicenetworklist)
{
	oam::DeviceNetworkList devicenetworklist = Devicenetworklist;

	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	SystemModuleTypeConfig systemmoduletypeconfig;
	SNMPManager aManager;
	int status = API_SUCCESS;
	bool exitThread = false;
	int exitThreadStatus = oam::API_SUCCESS;

	pthread_t ThreadId;
	ThreadId = pthread_self();

	log.writeLog(__LINE__, "stopSystemThread launched", LOG_TYPE_DEBUG);

	try{
		oam.getSystemConfig(systemmoduletypeconfig);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
		stopsystemthreadStatus = oam::API_FAILURE;
		processManager.setSystemState(oam::FAILED);
		exitThread = true;
		exitThreadStatus = oam::API_FAILURE;
	}
	catch(...)
	{
		log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		stopsystemthreadStatus = oam::API_FAILURE;
		processManager.setSystemState(oam::FAILED);
		exitThread = true;
		exitThreadStatus = oam::API_FAILURE;
	}

	if ( devicenetworklist.size() != 0 ) {
		// stop modules from devicenetworklist
		DeviceNetworkList::iterator listPT = devicenetworklist.begin();

		//launch start module threads, starting with local module
		pthread_t stopmodulethread;
		string moduleName = config.moduleName();
		int status = pthread_create (&stopmodulethread, NULL, (void*(*)(void*)) &stopModuleThread, &moduleName);
		
		if ( status != 0 )
			log.writeLog(__LINE__, "stopModuleThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

		for( ; listPT != devicenetworklist.end() ; listPT++)
		{
			string moduleName = (*listPT).DeviceName;

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

			pthread_t stopmodulethread;
			int status = pthread_create (&stopmodulethread, NULL, (void*(*)(void*)) &stopModuleThread, &moduleName);
			
			if ( status != 0 )
				log.writeLog(__LINE__, "stopModuleThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

			sleep(5);
		}
	}
	else {
		// stop all modules, like on a systemStart command
		//launch stop module threads, stoping with local module

		for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
		{
			int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
			if( moduleCount == 0)
				continue;
	
			DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
			{
				string moduleName = (*pt).DeviceName;
	
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

				pthread_t stopmodulethread;
				string name = moduleName;
				int status = pthread_create (&stopmodulethread, NULL, (void*(*)(void*)) &stopModuleThread, &name);

				if ( status != 0 )
					log.writeLog(__LINE__, "stopModuleThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

				usleep(50000);
			}
		}
	}

	// check status and process accordingly
	int k = 0;
	for( ; k < 1200 ; k++ )
	{
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
						status = API_FAILURE;
						break;
					}
	
					if (opState == oam::MAN_DISABLED || 
						opState == oam::AUTO_DISABLED ||
						opState == oam::MAN_OFFLINE)
						//skip
						continue;
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

		//get out of loop if all modules stopped successfully
		if( status == API_SUCCESS ) {
			break;
		}
		else
		{
			//get out of loop if stop module failed
			if( status == API_FAILURE ) {
				//set system status
				log.writeLog(__LINE__, "stopSystemThread: Module failed, Set System State to FAILED: " + moduleName , LOG_TYPE_CRITICAL);
				processManager.setSystemState(oam::FAILED);
				break;
			}
		}
		sleep(5);
	}
	
	if ( k == 1200 ) {
		// system didn't Successfully restart
		log.writeLog(__LINE__, "stopSystemThread: Modules failed to stop after 1200 tries, Set System State to FAILED" , LOG_TYPE_CRITICAL);
		processManager.setSystemState(oam::FAILED);
		status = oam::API_FAILURE;
	}
	else
	{
		processManager.setSystemState(oam::MAN_OFFLINE);
		status = oam::API_SUCCESS;
	}

	// exit thread
	stopsystemthreadStatus = status;
	log.writeLog(__LINE__, "stopSystemThread Exit", LOG_TYPE_DEBUG);
	pthread_detach (ThreadId);
	pthread_exit(0);
}

/*****************************************************************************************
* @brief	stopModuleThread
*
* purpose:	Send Messages to Module Process Monitors to stop Processes
*
*****************************************************************************************/
void stopModuleThread(string module)
{
	//store in a local variable
	string moduleName = module;

	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;

	pthread_t ThreadId;
	ThreadId = pthread_self();

	if ( moduleName.empty() ){
		log.writeLog(__LINE__, "stopModuleThread received on invalid module name", LOG_TYPE_ERROR);
		pthread_detach (ThreadId);
		pthread_exit(0);
	}

	log.writeLog(__LINE__, "Stop Module " + moduleName, LOG_TYPE_DEBUG);

	while(true)
	{
		// get module status
		try{
			int opState;
			bool degraded;
			oam.getModuleStatus(moduleName, opState, degraded);

			if (opState == oam::MAN_OFFLINE)
				//quit
				break;
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

		int retStatus = processManager.stopModule(moduleName, oam::GRACEFUL, true);
	
		log.writeLog(__LINE__, "ACK received from '" + moduleName + "' Process-Monitor, return status = " + oam.itoa(retStatus), LOG_TYPE_DEBUG);
	
		if (retStatus == API_SUCCESS)
			break;
		else
		{
			if (retStatus != API_MINOR_FAILURE) {
				//major failure, set stopsystem flag and exit this thread
				break;
			}
		}
	}

	// exit thread
	log.writeLog(__LINE__, "stopModuleThread Exit on " + moduleName, LOG_TYPE_DEBUG);
	pthread_detach (ThreadId);
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

													//check to see if DDL/DML IPs need to be updated
													if ( (*pt).DeviceName.find("um") == 0  && 			systemprocessconfig.processconfig[j].ProcessName == "DDLProc" ) {
														setPMProcIPs((*pt).DeviceName);
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
	int minPmPorts = 32;
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

	//retry 5 times loop just in case
	for(int i=0 ; i < 5; i++)
	{
		Config* sysConfig1 = Config::makeConfig();

		//update PM count if needed
		sysConfig1->setConfig("PrimitiveServers", "Count", oam.itoa(pmCount));
	
		int pmPorts = pmCount * (maxPMNicID*2);
		if ( pmPorts < minPmPorts )
			pmPorts = minPmPorts;

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
	idbassert(pt0 != moduleconfig.hostConfigList.end());
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
	string fileName = startup::StartUp::installDir() + "/etc/Calpont.xml";

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
	string fileName = startup::StartUp::installDir() + "/local/etc/" + moduleName + "/inittab.calpont";

	vector <string> lines;

	string init1 = "1" + systemID + ":2345:respawn:" + startup::StartUp::installDir() + "/bin/ProcMon " + parentOAMModuleHostName;
	
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
* @brief	setPMProcIPs
*
* purpose:	Updates the Calpont.xml file for DDL/DMLProc IPs during PM switchover
*
*
******************************************************************************************/
int ProcessManager::setPMProcIPs( std::string moduleName, std::string processName )
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	ModuleConfig moduleconfig;

	log.writeLog(__LINE__, "setPMProcIPs called for " + moduleName, LOG_TYPE_DEBUG);

	pthread_mutex_lock(&THREAD_LOCK);

	if ( processName == oam::UnassignedName || processName == "DDLProc")
	{
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
				if ( sysConfig2->getConfig("DDLProc", "IPAddr") == ipAdd ) {
					log.writeLog(__LINE__, "setPMProcIPs for DDLProc: no update needed", LOG_TYPE_DEBUG);	
					break;
				}	
		
				sysConfig2->setConfig("DDLProc", "IPAddr", ipAdd);
				try {
					sysConfig2->write();
	
					pthread_mutex_unlock(&THREAD_LOCK);
	
					log.writeLog(__LINE__, "setPMProcIPs: DDLProc to " + ipAdd, LOG_TYPE_DEBUG);
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
	}

	if ( processName == oam::UnassignedName || processName == "DMLProc")
	{
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
				if ( sysConfig2->getConfig("DMLProc", "IPAddr") == ipAdd ) {
					log.writeLog(__LINE__, "setPMProcIPs for DMLProc: no update needed, exiting function", LOG_TYPE_DEBUG);	
					pthread_mutex_unlock(&THREAD_LOCK);
					return API_SUCCESS;
				}	
		
				sysConfig2->setConfig("DMLProc", "IPAddr", ipAdd);
				try {
					sysConfig2->write();
	
					pthread_mutex_unlock(&THREAD_LOCK);
	
					log.writeLog(__LINE__, "setPMProcIPs: DMLProc to " + ipAdd, LOG_TYPE_DEBUG);
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

	log.writeLog(__LINE__, "distributeConfigFile called for " + name + " file = " + file, LOG_TYPE_DEBUG);

	string dirName = startup::StartUp::installDir() + "/etc/";
	string fileName = dirName + file;

	ifstream in (fileName.c_str());
	if (!in) {
		log.writeLog(__LINE__, "distributeConfigFile failed, file doesn't exist: " + fileName, LOG_TYPE_ERROR);
		return oam::API_FAILURE;
	}

	//skip any file of size 0
	in.seekg(0, std::ios::end);
	int size = in.tellg();
	if ( size == 0 ) {
		log.writeLog(__LINE__, "distributeConfigFile failed, file doesn't exist: " + fileName, LOG_TYPE_ERROR);
		return oam::API_FAILURE;
	}

	// distribute using hdfs call, make sure host names are in /etc/pdsh/machines
	ifstream in1 ("/etc/pdsh/machines");
	if (in1) {
		if ( HDFS )
		{
			if ( name == "system" )
			{
				string cmd = "pdcp -a -x " + localHostName + " " + fileName + " " + dirName;
				int rtnCode = system(cmd.c_str());
				if (WEXITSTATUS(rtnCode) == 0)
				{
					log.writeLog(__LINE__, "distributeConfigFile using pdcp successful on " + fileName, LOG_TYPE_DEBUG);
					return returnStatus;
				}
				else
				{
					log.writeLog(__LINE__, "distributeConfigFile using pdcp failed on " + fileName, LOG_TYPE_ERROR);
				}
			}
			else
			{
				// get module hostname
				ModuleConfig moduleconfig;
				oam.getSystemConfig(name, moduleconfig);
				HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
				string hostName = (*pt1).HostName;

				string cmd = "pdcp -w " + hostName + " " + fileName + " " + dirName;
				int rtnCode = system(cmd.c_str());
				if (WEXITSTATUS(rtnCode) == 0)
				{
					log.writeLog(__LINE__, "distributeConfigFile using pdcp successful on " + fileName, LOG_TYPE_DEBUG);
					return returnStatus;
				}
				else
				{
					log.writeLog(__LINE__, "distributeConfigFile using pdcp failed on " + fileName, LOG_TYPE_ERROR);
				}
			}
		}
	}

	//send via tcp messaging
	msg << requestID;
	msg << fileName;

	in.seekg(0, std::ios::beg);
	in >> msg;

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

				//skip if AOS
				int opState = oam::ACTIVE;
				bool degraded;
				try {
					oam.getModuleStatus((*pt).DeviceName, opState, degraded);
				}
				catch(...)
				{}
	
				if (opState == oam::AUTO_DISABLED)
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
int ProcessManager::getDBRMData(messageqcpp::IOSocket fIos, std::string moduleName)
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

	string fileName = startup::StartUp::installDir() + "/local/dbrmfiles";
	unlink(fileName.c_str());

	string cmd = "ls " + currentDbrmFile + "_* >> " + startup::StartUp::installDir() + "/local/dbrmfiles";
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
			pthread_mutex_unlock(&THREAD_LOCK);
			return oam::API_FAILURE;
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
			pthread_mutex_unlock(&THREAD_LOCK);
			return oam::API_FAILURE;
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

	string DBRootStorageType = "internal";
	{
		try{
			oam.getSystemConfig("DBRootStorageType", DBRootStorageType);
		}
		catch(...) {}
	}

	if ( DBRootStorageType == "internal" && GlusterConfig == "n") {
		log.writeLog(__LINE__, "ERROR: DBRootStorageType = internal", LOG_TYPE_ERROR);
		pthread_mutex_unlock(&THREAD_LOCK);
		return API_INVALID_PARAMETER;
	}

	// set alarm
	aManager.sendAlarmReport(newActiveModuleName.c_str(), MODULE_SWITCH_ACTIVE, SET);

	//clear run standby flag;
	runStandby = false;

	int moduleID = atoi(newActiveModuleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());

	// update Calpont.xml entries
	string newActiveIPaddr;
	try
	{
		pthread_mutex_lock(&THREAD_LOCK);

		//move a newparent dbroot to old parent for balancing
		DBRootConfigList residedbrootConfigList;
		try
		{
			oam.getPmDbrootConfig(moduleID, residedbrootConfigList);

			DBRootConfigList::iterator pt = residedbrootConfigList.begin();
			try {
				oam.manualMovePmDbroot(newActiveModuleName, oam.itoa(*pt), config.OAMParentName());
			}
			catch (...)
			{
				log.writeLog(__LINE__, "ERROR: manualMovePmDbroot Failed", LOG_TYPE_ERROR);
				pthread_mutex_unlock(&THREAD_LOCK);
				return API_FAILURE;
			}
		}
		catch (...)
		{
			log.writeLog(__LINE__, "ERROR: getPmDbrootConfig Failed", LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_FAILURE;
		}

		//move dbroot #1 to new parent
		try {
			oam.manualMovePmDbroot(config.OAMParentName(), "1", newActiveModuleName);
		}
		catch (...)
		{
			log.writeLog(__LINE__, "ERROR: manualMovePmDbroot Failed", LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_FAILURE;
		}

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

		if ( config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM )
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

	// stop local SNMPTrapDaemon
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

	//DOING THIS JUST TO UPDATE THE TIMESTAMP OF THE CALPONT.XML FILE AS A WORK-AROUND FIX 
	//BECAUSE PROCMON ISN'T READING UPDATES FROM DISK ON HDFS SYSTEMS

	if (HDFS)
	{
		sleep(60);
		Config* sysConfig = Config::makeConfig();
		try {
			sysConfig->write();
		}
		catch(...)
		{
			log.writeLog(__LINE__, "ERROR: sysConfig->write", LOG_TYPE_ERROR);
			pthread_mutex_unlock(&THREAD_LOCK);
			return API_FAILURE;
		}
	}

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
	string downOAMParentHostname;
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
				downOAMParentHostname = (*pt1).HostName;
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

	// dbroot storage type, do different failover if internal
	string DBRootStorageType = "internal";
	{
		try{
			oam.getSystemConfig("DBRootStorageType", DBRootStorageType);
		}
		catch(...) {}
	}

	string cmdLine = "ping ";
	string cmdOption = " -c 1 -w 5 >> /dev/null";
	string cmd;

	int pingFailure = 0;
	bool failover = false;
	bool recoveryTest = false;
	int disableCount = 0;
	int noAckCount = 0;
	bool amazonParentRestart = false;

	while(!failover)
	{
		// check if a signal was received to start failover
		if (startFailOver) {
			//send notification going from standby to active
			oam.sendDeviceNotification(config.moduleName(), START_STANDBY_TO_MASTER);
			break;
		}

		// perform ping test of Active Parent Module
		string cmd = cmdLine + downOAMParentIPAddress + cmdOption;
		int rtnCode = system(cmd.c_str());

		switch (WEXITSTATUS(rtnCode)) {
			case 0:
			{
				//Ack ping
				pingFailure = 0;
				if ( noAckCount != 0 )
					oam.sendDeviceNotification(config.moduleName(), MODULE_UP);
				noAckCount = 0;

				//if Amazon Parent PM is restarting, monitor when back active and take needed actions
				if (amazonParentRestart)
				{
					log.writeLog(__LINE__, "Amazon Parent pinging, waiting until it's active", LOG_TYPE_DEBUG);
					sleep(60);

					while(true)
					{
						SystemStatus systemstatus;
						try {
							oam.getSystemStatus(systemstatus);
						}
						catch(...)
						{}

						if (systemstatus.SystemOpState == ACTIVE) {
							log.writeLog(__LINE__, "System Active, restart needed processes", LOG_TYPE_DEBUG);

							processManager.restartProcessType("mysql");
							processManager.restartProcessType("ExeMgr");
							processManager.restartProcessType("WriteEngineServer");
							processManager.reinitProcessType("DBRMWorkerNode");
							sleep(1);
							processManager.restartProcessType("DDLProc");
							sleep(1);
							processManager.restartProcessType("DMLProc");

							amazonParentRestart = false;
							break;
						}

						sleep(5);
					}
				}

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
							
									switch (WEXITSTATUS(rtnCode)) {
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
					}
				}
				else
				{
					//send notification going from standby to active
					oam.sendDeviceNotification(config.moduleName(), START_STANDBY_TO_MASTER);
				}
			}
			catch (exception& ex)
			{}

			//do amazon failover
			if (amazon)
			{
				log.writeLog(__LINE__, " ", LOG_TYPE_DEBUG);
				log.writeLog(__LINE__, "*** OAMParentModule outage, recover the Instance ***", LOG_TYPE_DEBUG);
	
				string currentIPAddr = oam.getEC2InstanceIpAddress(downOAMParentHostname);
				if (currentIPAddr == "stopped")
				{ // start instance
					int retryCount = 6;		// 1 minutes
					if ( PMInstanceType == "m2.4xlarge" )
						retryCount = 15;		// 2.5 minutes

					log.writeLog(__LINE__, "Instance in stopped state, try starting it: " + downOAMParentHostname, LOG_TYPE_DEBUG);
					int retry = 0;
					for (  ; retry < retryCount ; retry++ )
					{
						if ( oam.startEC2Instance(downOAMParentHostname) )
						{
							log.writeLog(__LINE__, "Instance started, sleep for 30 seconds to allow it to fully come up: " + downOAMParentHostname, LOG_TYPE_DEBUG);
	
							//delay then get new IP Address
							sleep(30);
							string currentIPAddr = oam.getEC2InstanceIpAddress(downOAMParentHostname);
							if (currentIPAddr == "stopped" || currentIPAddr == "terminated") {
								log.writeLog(__LINE__, "Instance failed to start (no ip-address), retry: " + downOAMParentHostname, LOG_TYPE_DEBUG);
							}
							else
							{
								// update the Calpont.xml with the new IP Address
								string cmd = "sed -i s/" + downOAMParentIPAddress + "/" + currentIPAddr + "/g " +  startup::StartUp::installDir() + "/etc/Calpont.xml";
								system(cmd.c_str());

								// get parent hotsname and IP address in case it changed
								downOAMParentIPAddress = currentIPAddr;

								amazonParentRestart = true;

								break;
							}
						}
						else
						{
							log.writeLog(__LINE__, "Instance failed to start, retry: " + downOAMParentHostname, LOG_TYPE_DEBUG);

							sleep(5);
						}
					}
	
					if ( retry >= retryCount )
					{
						log.writeLog(__LINE__, "Instance failed to start, restart a new instance: " + downOAMParentHostname, LOG_TYPE_DEBUG);
						currentIPAddr = "terminated";
					}
				}
	
				if ( currentIPAddr != "terminated")
				{
					log.writeLog(__LINE__, "Instance rebooting, monitor", LOG_TYPE_DEBUG);

					//clear and go monitor again
					failover = false;

					amazonParentRestart = true;
				}
				else
					log.writeLog(__LINE__, "Instance terminated, do standby-active failover", LOG_TYPE_DEBUG);

			}

			if ( DBRootStorageType == "internal" && failover && GlusterConfig == "n")
			{
				log.writeLog(__LINE__, "DBRoot Storage configured for internal, don't do standby-active failover", LOG_TYPE_DEBUG);

				//clear and go monitor again
				failover = false;
			}
		}
	}

	log.writeLog(__LINE__, " ", LOG_TYPE_DEBUG);
	log.writeLog(__LINE__, "*** OAMParentModule outage, OAM Parent Module change-over started ***", LOG_TYPE_DEBUG);

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

		//clear run standby flag;
		runStandby = false;

		//sleep, give time for message thread to startup
		sleep(5);

		try {
			oam.autoMovePmDbroot(downOAMParentName);
		}
		catch (...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on autoMovePmDbroot: Caught unknown exception!", LOG_TYPE_ERROR);
		}

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

	if ( config.ServerInstallType() == oam::INSTALL_COMBINE_DM_UM_PM )
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

	// graceful start snmptrap-daemon
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

	//restart/reinit processes to force their release of the controller node port
	processManager.restartProcessType("mysql");
	processManager.restartProcessType("ExeMgr");
	processManager.restartProcessType("WriteEngineServer");

	processManager.reinitProcessType("DBRMWorkerNode");

	// stop local processes to restart after controller node and load_brm
	stopProcess(config.moduleName(), "DBRMWorkerNode", oam::FORCEFUL, true);
	stopProcess(config.moduleName(), "ExeMgr", oam::FORCEFUL, true);
	stopProcess(config.moduleName(), "WriteEngineServer", oam::FORCEFUL, true);
	stopProcess(config.moduleName(), "DDLProc", oam::FORCEFUL, true);
	stopProcess(config.moduleName(), "DMLProc", oam::FORCEFUL, true);

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

			if (opState != oam::MAN_DISABLED) {
				if (opState != oam::AUTO_DISABLED) {
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
	}

	//wait until local module is active before continuing
	while(true)
	{
		int opState = oam::ACTIVE;
		bool degraded;
		try {
			oam.getModuleStatus(config.moduleName(), opState, degraded);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + config.moduleName() + ": Caught unknown exception!", LOG_TYPE_ERROR);
		}

		if (opState == oam::ACTIVE)
			break;

		sleep(5);
	}

	//restart DDLProc/DMLProc to perform any rollbacks, if needed
	//dont rollback in amazon, wait until down pm recovers
	if ( ( config.ServerInstallType() != oam::INSTALL_COMBINE_DM_UM_PM  ) &&
			!amazon) {
		processManager.restartProcessType("DDLProc");
		sleep(1);
		processManager.restartProcessType("DMLProc");
	}

// don't set to active, let procmon set to active when DMLProc is active
//	processManager.setSystemState(oam::ACTIVE);

	// clear alarm
	aManager.sendAlarmReport(config.moduleName().c_str(), MODULE_SWITCH_ACTIVE, CLEAR);

	if (amazon) {
		//Set the down module instance state so it will be auto restarted 
		processManager.setModuleState(downOAMParentName, oam::AUTO_OFFLINE);

		// sleep to give time for local pm to fully go active
		sleep(30);
	}

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
				backupStandbyModule == "NONE" &&
				newStandbyModule == "NONE" )
				// Found a ProcessManager in a MAN_OFFLINE state, use if no COLD_STANDBY is found
				// and module is not disabled
				{
					int opState;
					bool degraded;
					try {
						oam.getModuleStatus(systemprocessstatus.processstatus[i].Module, opState, degraded);
					}
					catch(...)
					{}

					if (opState == oam::MAN_DISABLED || opState == oam::AUTO_DISABLED) {
						continue;
					}
					else
						backupStandbyModule = systemprocessstatus.processstatus[i].Module;
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

	if ( newStandbyModule.empty() )
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
* purpose:	set Enable State info in Calpont.xml
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

	pthread_t ThreadId;
	ThreadId = pthread_self();

	// wait until DMLProc is ACTIVE
	while(true)
	{
		try{
			ProcessStatus procstat;
			oam.getProcessStatus("WriteEngineServer", config.moduleName(), procstat);
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

	if ( exitThread ) {
		pthread_detach (ThreadId);
		pthread_exit(reinterpret_cast<void*>(static_cast<ptrdiff_t>(exitThreadStatus)));
	}

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

	pthread_detach (ThreadId);
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

	//front-end first
	processManager.stopProcessType("mysql", manualFlag);
	processManager.stopProcessType("DMLProc", manualFlag);
	processManager.stopProcessType("DDLProc", manualFlag);
	processManager.stopProcessType("ExeMgr", manualFlag);

	//back-end
	processManager.stopProcessType("WriteEngineServer", manualFlag);
	processManager.stopProcessType("PrimProc", manualFlag);

	//dbrm
	processManager.stopProcessType("DBRMControllerNode", manualFlag);
	processManager.stopProcessType("DBRMWorkerNode", manualFlag);

	log.writeLog(__LINE__, "stopProcessTypes Completed");
}

/******************************************************************************************
* @brief	unmountDBRoot
*
* purpose:	unmount a dbroot
*
*
******************************************************************************************/
int ProcessManager::unmountDBRoot(std::string dbrootID)
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;

	//get pm assigned to that dbroot
	int pmID;
	oam.getDbrootPmConfig(atoi(dbrootID.c_str()), pmID);
	string moduleName = "pm" + oam.itoa(pmID);

	log.writeLog(__LINE__, "send unmountDBRoot to pm: " + dbrootID + "/" + moduleName, LOG_TYPE_DEBUG );

	ByteStream msg;
	msg << (ByteStream::byte) PROCUNMOUNT;
	msg << dbrootID;

	return sendMsgProcMon( moduleName, msg, PROCUNMOUNT );

}

/******************************************************************************************
* @brief	mountDBRoot
*
* purpose:	mount a dbroot
*
*
******************************************************************************************/
int ProcessManager::mountDBRoot(std::string dbrootID)
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;

	if (GlusterConfig == "y")
		return oam::API_SUCCESS;

	//get pm assigned to that dbroot
	int pmID;
	oam.getDbrootPmConfig(atoi(dbrootID.c_str()), pmID);
	string moduleName = "pm" + oam.itoa(pmID);

	log.writeLog(__LINE__, "send mountDBRoot to pm: " + dbrootID + "/" + moduleName, LOG_TYPE_DEBUG );

	//send msg to ProcMon if not local module
	if ( config.moduleName() == moduleName ) {
		string cmd = "mount " + startup::StartUp::installDir() + "/data" + dbrootID + " > /dev/null";
		system(cmd.c_str());

		if ( !rootUser) {
			cmd = "sudo chown -R " + USER + ":" + USER + " " + startup::StartUp::installDir() + "/data" + dbrootID + " > /dev/null";
			system(cmd.c_str());
		}

		ifstream in("/tmp/mount.txt");

		in.seekg(0, std::ios::end);
		int size = in.tellg();
		if ( size != 0 )
		{
			if (!oam.checkLogStatus("/tmp/mount.txt", "already")) {
				log.writeLog(__LINE__, "mount failed, dbroot: " + dbrootID);
				return API_FAILURE;
			}
		}
	}
	else
	{
		ByteStream msg;
		msg << (ByteStream::byte) PROCMOUNT;
		msg << dbrootID;

		return sendMsgProcMon( moduleName, msg, PROCMOUNT );
	}

	return oam::API_SUCCESS;
}

/******************************************************************************************
* @brief	flushInodeCache
*
* purpose:	flush cache
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


} //end of namespace
// vim:ts=4 sw=4:

