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

/*****************************************************************************************
* $Id: main.cpp 2203 2013-07-08 16:50:51Z bpaul $
*
*****************************************************************************************/


#include <clocale>

#include <boost/filesystem.hpp>

#include "processmanager.h"
#include "installdir.h"

#include "utils_utf8.h"

using namespace std;
using namespace logging;
using namespace messageqcpp;
using namespace processmanager;
using namespace oam;
using namespace snmpmanager;
using namespace threadpool;
//using namespace procheartbeat;
using namespace config;

bool runStandby = false;
bool runCold = false;
string systemName = "system";
string iface_name;
string cloud;
bool amazon = false;
string PMInstanceType;
string UMInstanceType;
string GlusterConfig = "n";
bool rootUser = true;
string USER = "root";
bool HDFS = false;
string localHostName;
string PMwithUM = "n";

// pushing the ACTIVE_ALARMS_FILE to all nodes every 10 seconds.
const int ACTIVE_ALARMS_PUSHING_INTERVAL = 10;

typedef   map<string, int>	moduleList;
moduleList	moduleInfoList;

extern HeartBeatProcList hbproclist;
extern pthread_mutex_t THREAD_LOCK;
extern bool startsystemthreadStop;
extern string gdownActiveOAMModule;
extern int startsystemthreadStatus;
extern vector<string> downModuleList;
extern bool startFailOver;
extern bool gOAMParentModuleFlag;

static void messageThread(Configuration config);
static void sigUser1Handler(int sig);
static void startMgrProcessThread();
static void hdfsActiveAlarmsPushingThread();
//static void pingDeviceThread();
//static void heartbeatProcessThread();
//static void heartbeatMsgThread();

/*****************************************************************************************
* @brief	main
*
* purpose:	request launching of Mgr controlled processes and wait for incoming messages
*
*****************************************************************************************/
int main(int argc, char **argv)
{
#ifndef _MSC_VER
    setuid(0); // set effective ID to root; ignore return status
#endif
	// get and set locale language
	string systemLang = "C";

	Oam oam;
	systemLang = funcexp::utf8::idb_setlocale();

	//check if root-user
	int user;
	user = getuid();
	if (user != 0)
		rootUser = false;

	char* p= getenv("USER");
	if (p && *p)
   		USER = p;

	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	SNMPManager aManager;

	log.writeLog(__LINE__, " ");
	log.writeLog(__LINE__, "**********Process Manager Started**********");

	//Ignore SIGPIPE signals
	signal(SIGPIPE, SIG_IGN);

	//Ignore SIGHUP signals
	signal(SIGHUP, SIG_IGN);

	//create SIGUSR1 handler to get configuration updates
	signal(SIGUSR1, sigUser1Handler);

	// Get System Name
	try{
		oam.getSystemConfig("SystemName", systemName);
	}
	catch(...)
	{}

	//get cloud setting
	try {
		oam.getSystemConfig( "Cloud", cloud);
	}
	catch(...) {}

	//get amazon parameters
	if ( cloud == "amazon-ec2" || cloud == "amazon-vpc" )
	{
		oam.getSystemConfig("PMInstanceType", PMInstanceType);
		oam.getSystemConfig("UMInstanceType", UMInstanceType);
		amazon = true;
	}

	//get gluster config
	try {
		oam.getSystemConfig( "GlusterConfig", GlusterConfig);
	}
	catch(...)
	{
		GlusterConfig = "n";
	}

	//hdfs / hadoop config 
	string DBRootStorageType;
	try {
		oam.getSystemConfig( "DBRootStorageType", DBRootStorageType);
	}
	catch(...) {}

	if ( DBRootStorageType == "hdfs" )
		HDFS = true;

	//PMwithUM config 
	try {
		oam.getSystemConfig( "PMwithUM", PMwithUM);
	}
	catch(...) {
		PMwithUM = "n";
	}

	// get system uptime and alarm if this is a restart after module outage
	if ( gOAMParentModuleFlag ) {

		//clear alarm
		aManager.sendAlarmReport(systemName.c_str(), EE_LICENSE_EXPIRED, CLEAR);

		log.writeLog(__LINE__, "Running Active");

		//get the system status
		SystemStatus systemstatus;
		try {
			oam.getSystemStatus(systemstatus);
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
	}
	else
	{
		log.writeLog(__LINE__, "Running Standby");
		runStandby = true;
	}

	//get local module main IP address
	ModuleConfig moduleconfig;
	oam.getSystemConfig(config.moduleName(), moduleconfig);
	HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
	string localIPaddr = (*pt1).IPAddr;
	localHostName = (*pt1).HostName;

	struct ifaddrs *addrs, *iap;
	struct sockaddr_in *sa;
	char buf[32];
	
	getifaddrs(&addrs);
	for (iap = addrs; iap != NULL; iap = iap->ifa_next) 
	{
	
		if (iap->ifa_addr && (iap->ifa_flags & IFF_UP) && iap->ifa_addr->sa_family == AF_INET) 
		{
			sa = (struct sockaddr_in *)(iap->ifa_addr);
			inet_ntop(iap->ifa_addr->sa_family, (void *)&(sa->sin_addr), buf, sizeof(buf));
			if (!strcmp(localIPaddr.c_str(), buf)) 
			{
				iface_name = iap->ifa_name;
				break;
			}
		}
	}
	freeifaddrs(addrs);
	log.writeLog(__LINE__, "Main Ethernet Port = " + iface_name, LOG_TYPE_DEBUG);

	//
	//start a thread to ping all system modules
	//
	if (runStandby) {
		//running standby after startup
		try {
			oam.processInitComplete("ProcessManager", oam::STANDBY);
			log.writeLog(__LINE__, "processInitComplete Successfully Called", LOG_TYPE_DEBUG);
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on processInitComplete: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on processInitComplete: Caught unknown exception!", LOG_TYPE_ERROR);
		}

		// create message thread
		pthread_t MessageThread;
		int ret = pthread_create (&MessageThread, NULL, (void*(*)(void*)) &messageThread, &config);
		if ( ret != 0 )
			log.writeLog(__LINE__, "pthread_create failed, return code = " + oam.itoa(ret), LOG_TYPE_ERROR);

		//monitor OAM Parent Module for failover
		while(true)
		{
			if ( processManager.OAMParentModuleChange() == oam::API_SUCCESS )
				break;
			log.writeLog(__LINE__, "OAMParentModuleChange failure", LOG_TYPE_WARNING);
			// GO TRY AGAIN
		}

		pthread_t srvThread;
		int status = pthread_create (&srvThread, NULL, (void*(*)(void*)) &pingDeviceThread, NULL);

		if ( status != 0 )
			log.writeLog(__LINE__, "pingDeviceThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);
	}
	else
	{ //running active after startup
		//Update DBRM section of Calpont.xml
		processManager.updateWorkerNodeconfig();
//		processManager.distributeConfigFile("system");

		pthread_t srvThread;
		int status = pthread_create (&srvThread, NULL, (void*(*)(void*)) &pingDeviceThread, NULL);

		if ( status != 0 )
			log.writeLog(__LINE__, "pingDeviceThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);

		// if HDFS, create a thread to push an image of activeAlarms to HDFS filesystem
		if (HDFS) {
			pthread_t hdfsAlarmThread;
			int status = pthread_create(&hdfsAlarmThread, NULL, (void*(*)(void*)) &hdfsActiveAlarmsPushingThread, NULL);
			if ( status != 0 )
				log.writeLog(__LINE__, "hdfsActiveAlarmsPushingThread pthread_create failed, return code = " + oam.itoa(status), LOG_TYPE_ERROR);
		}

		sleep(5);

		SystemStatus systemstatus;
		try {
			oam.getSystemStatus(systemstatus);
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
		
		if (systemstatus.SystemOpState != oam::MAN_OFFLINE &&
			systemstatus.SystemOpState != oam::ACTIVE) {
			pthread_t mgrProcThread;
			int status = pthread_create (&mgrProcThread, NULL, (void*(*)(void*)) &startMgrProcessThread, NULL);

			if ( status != 0 )
				log.writeLog(__LINE__, "startMgrProcessThread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);
		}

		try {
			oam.processInitComplete("ProcessManager");
			log.writeLog(__LINE__, "processInitComplete Successfully Called", LOG_TYPE_DEBUG);
		}
		catch (exception& ex)
		{
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on processInitComplete: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on processInitComplete: Caught unknown exception!", LOG_TYPE_ERROR);
		}

		//make sure ProcMgr IP Address is configured correctly
		try
		{
			Config* sysConfig = Config::makeConfig();
		
			// get Standby IP address
			ModuleConfig moduleconfig;
			oam.getSystemConfig(config.moduleName(), moduleconfig);
			HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
			string IPaddr = (*pt1).IPAddr;
	
			sysConfig->setConfig("ProcMgr", "IPAddr", IPaddr);
	
			log.writeLog(__LINE__, "set ProcMgr IPaddr to " + IPaddr, LOG_TYPE_DEBUG);
			//update Calpont Config table
			try {
				sysConfig->write();
			}
			catch(...)
			{
				log.writeLog(__LINE__, "ERROR: sysConfig->write", LOG_TYPE_ERROR);
			}
		}
		catch(...)
		{
			log.writeLog(__LINE__, "ERROR: makeConfig failed", LOG_TYPE_ERROR);
		}
	
		try {
			oam.distributeConfigFile();
		}
		catch(...) 
		{}

		// create message thread
		pthread_t MessageThread;
		int ret = pthread_create (&MessageThread, NULL, (void*(*)(void*)) &messageThread, &config);
		if ( ret != 0 )
			log.writeLog(__LINE__, "pthread_create failed, return code = " + oam.itoa(ret), LOG_TYPE_ERROR);
	}

	//
	//start a thread to process heartbeat checks
	//
//	pthread_t heartThread;
//	pthread_create (&heartThread, NULL, (void*(*)(void*)) &heartbeatProcessThread, NULL);

	//
	//start a thread to read heartbeat messages
	//
//	pthread_t heartMsgThread;
//	pthread_create (&heartMsgThread, NULL, (void*(*)(void*)) &heartbeatMsgThread, NULL);

	// suspend forever
	while(true)
	{
		sleep(1000);
	}
}

/******************************************************************************************
* @brief	messageThread
*
* purpose:	Read incoming messages
*
******************************************************************************************/
static void messageThread(Configuration config)
{
	ProcessLog log;
	ProcessManager processManager(config, log);
	Oam oam;

	//check for running active, then launch
	while(true)
	{
		if ( !runStandby)
			break;
		sleep (1);
	}

	log.writeLog(__LINE__, "Message Thread started ..", LOG_TYPE_DEBUG);

	//read and cleanup port before trying to use
	try {
		Config* sysConfig = Config::makeConfig();
		string port = sysConfig->getConfig("ProcMgr", "Port");
		string cmd = "fuser -k " + port + "/tcp >/dev/null 2>&1";
		if ( !rootUser)
			cmd = "sudo fuser -k " + port + "/tcp >/dev/null 2>&1";


		system(cmd.c_str());
	}
	catch(...)
	{
	}

	//
	//waiting for request 	
	//
	IOSocket fIos;

	for (;;)
	{
		try
		{
			MessageQueueServer procmgr("ProcMgr");
			for (;;)
			{
				try
				{
					fIos = procmgr.accept();

					pthread_t messagethread;
					int status = pthread_create (&messagethread, NULL, (void*(*)(void*)) &processMSG, &fIos);

					if ( status != 0 )
						log.writeLog(__LINE__, "messagethread: pthread_create failed, return status = " + oam.itoa(status), LOG_TYPE_ERROR);
				}
				catch(...)
				{}

			}
		}
        catch (exception& ex)
        {
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueServer for ProcMgr:" + error, LOG_TYPE_ERROR);

			// takes 2 - 4 minites to free sockets, sleep and retry
			sleep(60);
        }
        catch(...)
        {
			log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueServer for ProcMgr: Caught unknown exception!", LOG_TYPE_ERROR);

			// takes 2 - 4 minites to free sockets, sleep and retry
			sleep(60);
        }
	}
	return;
}

/******************************************************************************************
* @brief	sigUser1Handler
*
* purpose:	Handler SIGUSER1 signal and initial failover
*
******************************************************************************************/
static void sigUser1Handler(int sig)
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	log.writeLog(__LINE__, "SIGUSER1 received, set startFailOver = true", LOG_TYPE_DEBUG);

	startFailOver = true;
}

/*****************************************************************************************
* @brief	Start Mgr Process by module Thread
*
* purpose:	Send Messages to Module Process Monitors to start Processes
*
*****************************************************************************************/
static void startMgrProcessThread()
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	SystemModuleTypeConfig systemmoduletypeconfig;
	ModuleTypeConfig PMSmoduletypeconfig;
	SNMPManager aManager;

	log.writeLog(__LINE__, "startMgrProcessThread launched", LOG_TYPE_DEBUG);

//	processManager.setSystemState(oam::MAN_INIT);

	//get calpont software version and release
	SystemSoftware systemsoftware;
	string softwareVersion;
	string softwareRelease;

	try
	{
		oam.getSystemSoftware(systemsoftware);

		softwareVersion = systemsoftware.Version;
		softwareRelease = systemsoftware.Release;
	}
	catch (exception& e) {
		cout << endl << "ProcMon Construct Error reading getSystemSoftware = " << e.what() << endl;
		exit(-1);
	}

	string localSoftwareInfo = softwareVersion + softwareRelease;

	//get systemStartupOffline
	string systemStartupOffline = "n";
	try {
		Config* sysConfig = Config::makeConfig();
	
		systemStartupOffline = sysConfig->getConfig("Installation", "SystemStartupOffline");
	}
	catch(...)
	{
		log.writeLog(__LINE__, "ERROR: Problem getting systemStartupOffline from the Calpont System Configuration file", LOG_TYPE_ERROR);
		systemStartupOffline = "n";
	}

	if ( systemStartupOffline == "y" )
		log.writeLog(__LINE__, "SystemStartupOffline set to 'y', Not starting up Calpont Database Processes", LOG_TYPE_INFO);

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

	//wait until all modules are up after a system reboot
	int i = 0;
	for( ; i < 100 ; i++ )
	{
		if ( startsystemthreadStop ) {
			processManager.setSystemState(oam::MAN_OFFLINE);
	
			// exit thread
			log.writeLog(__LINE__, "startMgrProcessThread Exit with a stop system flag", LOG_TYPE_DEBUG);
			pthread_exit(0);
		}

		int status = API_SUCCESS;
		for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
		{
			if ( systemmoduletypeconfig.moduletypeconfig[i].ModuleType == "pm" )
				PMSmoduletypeconfig = systemmoduletypeconfig.moduletypeconfig[i];

			int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
			if( moduleCount == 0)
				continue;

			DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
			{
				string moduleName = (*pt).DeviceName;

				// Is Module UP
				try{
					bool degraded;
					int opState;
					oam.getModuleStatus(moduleName, opState, degraded);

					if ( opState == oam::MAN_DISABLED )
						//mark all processes running on module man-offline except ProcMon
						processManager.setProcessStates(moduleName, oam::MAN_OFFLINE);

					if ( opState == oam::AUTO_DISABLED)
						//mark all processes running on module auto-offline
						processManager.setProcessStates(moduleName, oam::AUTO_OFFLINE);

					if (opState == oam::INITIAL ||
						opState == oam::DOWN) {
						//a module is not up
						status = API_MINOR_FAILURE;
						break;
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
			if ( status == API_MINOR_FAILURE) {
				sleep(1);
				break;
			}
		}
		if ( status == API_SUCCESS)
			//all modules are up
			break;
	}

	if ( i == 100 ) {
		// system didn't successfull restart
		processManager.setSystemState(oam::FAILED);

		// exit thread
		log.writeLog(__LINE__, "startMgrProcessThread Exit with a failure, not all modules are UP", LOG_TYPE_CRITICAL);
		pthread_exit(0);
	}

	//configure the PMS settings
	processManager.updatePMSconfig();

	if (HDFS)
		//distribute config file
		processManager.distributeConfigFile("system");

	//now wait until all procmons are up and validate rpms on each module
	int status = API_SUCCESS;
	int k = 0;
	for( ; k < 1200 ; k++ )
	{
		if ( startsystemthreadStop ) {
			processManager.setSystemState(oam::MAN_OFFLINE);
	
			// exit thread
			log.writeLog(__LINE__, "startMgrProcessThread Exit with a stop system flag", LOG_TYPE_DEBUG);
			pthread_exit(0);
		}

		status = API_SUCCESS;
		for( unsigned int i = 0 ; i < systemmoduletypeconfig.moduletypeconfig.size(); i++)
		{
			int moduleCount = systemmoduletypeconfig.moduletypeconfig[i].ModuleCount;
			if( moduleCount == 0)
				continue;

			DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for ( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end(); pt++)
			{
				string moduleName = (*pt).DeviceName;
				if ( (*pt).DisableState == oam::MANDISABLEDSTATE ||
						(*pt).DisableState == oam::AUTODISABLEDSTATE )
					continue;

				int moduleOpState;
				// check module state
				try{
					bool degraded;
					oam.getModuleStatus(moduleName, moduleOpState, degraded);

					// if up, set to MAN_INIT
					if ( HDFS && 
						(moduleOpState == oam::UP) )
					{
						processManager.setModuleState(moduleName, oam::MAN_INIT);
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

				// Is Module's ProcMon ACTIVE and module status has been updated
				int opState;
				try {
					ProcessStatus procstat;
					oam.getProcessStatus("ProcessMonitor", moduleName, procstat);
					opState = procstat.ProcessOpState;
		
					if (opState != oam::ACTIVE) {
						//skip if Not ACTIVE
						log.writeLog(__LINE__, "Module ProcMon not active yet: " + moduleName, LOG_TYPE_DEBUG);
						status = API_MINOR_FAILURE;
						continue;
					}
				}
				catch (exception& ex)
				{
					string error = ex.what();
					log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
					status = API_MINOR_FAILURE;
					continue;
				}
				catch(...)
				{
					log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
					status = API_MINOR_FAILURE;
					continue;
				}

				//ProcMon ACTIVE, validate the software release and version of that module
				ByteStream msg;
				ByteStream::byte requestID = GETSOFTWAREINFO;
				msg << requestID;

				string moduleSoftwareInfo = processManager.sendMsgProcMon1( moduleName, msg, requestID );
				if ( moduleSoftwareInfo == "FAILED" )
					continue;

				if ( localSoftwareInfo != moduleSoftwareInfo ) {
					// module not running on same Calpont Software build as this local Director
					// alarm and fail the module
					log.writeLog(__LINE__, "Software Info mismatch : " + moduleName + "/" + localSoftwareInfo + "/" + moduleSoftwareInfo, LOG_TYPE_ERROR);	

					aManager.sendAlarmReport(moduleName.c_str(), INVALID_SW_VERSION, SET);
					processManager.setModuleState(moduleName, oam::FAILED);
					status = API_FAILURE;
					break;
				}
			}
		}

		//get out of loop if all modules ACTTVE or MAN_OFFLINE
		if( status == API_SUCCESS ) {
			if ( systemStartupOffline == "y" ) {
				processManager.setSystemState(oam::MAN_OFFLINE);
				log.writeLog(__LINE__, "SystemStartupOffline set to 'y', Not starting up Calpont Database Processes", LOG_TYPE_DEBUG);
			}
			break;
		}
		else
		{
			//get out of loop if start module failed
			if( status == API_FAILURE )
				break;

			//retry after sleeping for a bit
			sleep(1);
		}
	}

	if ( k == 1200 || status == API_FAILURE) {
		// system didn't successfull restart
		processManager.setSystemState(oam::FAILED);
		// exit thread
		log.writeLog(__LINE__, "startMgrProcessThread Exit with a failure, not all ProcMons ACTIVE", LOG_TYPE_CRITICAL);
		log.writeLog(__LINE__, "startMgrProcessThread Exit - failure", LOG_TYPE_DEBUG);
		pthread_exit(0);
	}
	else
	{
		//distribute config file
//		processManager.distributeConfigFile("system");

		if ( systemStartupOffline == "n" && status == API_SUCCESS ) {
			oam::DeviceNetworkList devicenetworklist;
			pthread_t startsystemthread;
			int status = pthread_create (&startsystemthread, NULL, (void*(*)(void*)) &startSystemThread, &devicenetworklist);
		
			if ( status != 0 ) {
				log.writeLog(__LINE__, "STARTSYSTEMS: pthread_create failed, return status = " + oam.itoa(status));
				status = API_FAILURE;
			}
		
			if (status == 0)
			{
				pthread_join(startsystemthread, NULL);
				status = startsystemthreadStatus;
			}
		
			if ( status != API_SUCCESS ) {
				// system didn't successfull restart
				processManager.setSystemState(oam::FAILED);
				log.writeLog(__LINE__, "startMgrProcessThread Exit with a failure, error returned from startSystemThread", LOG_TYPE_CRITICAL);
			}
			else
				//distribute config file
				processManager.distributeConfigFile("system");
		}
	}

	// exit thread
	log.writeLog(__LINE__, "startMgrProcessThread Exit", LOG_TYPE_DEBUG);
	pthread_exit(0);
}


/*****************************************************************************************
* @brief	pingDeviceThread
*
* purpose:	perform ping testing on the devices within the system
*
*****************************************************************************************/
void pingDeviceThread()
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	ModuleTypeConfig moduletypeconfig;
	SNMPManager aManager;
	BRM::DBRM dbrm;

	log.writeLog(__LINE__, "pingDeviceThread launched", LOG_TYPE_DEBUG);

	string cmdLine = "ping ";
	string cmdOption = " -c 1 -w 5 >> /dev/null";
	string cmd;
	string deviceIP;

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

	//Build the initial list, clear module state 

	for ( unsigned int i = 0 ; i < systemModuleTypeConfig.moduletypeconfig.size(); i++)
	{
		int moduleCount = systemModuleTypeConfig.moduletypeconfig[i].ModuleCount;
		if ( moduleCount == 0 )
			// skip of no modules configured
			continue;

		DeviceNetworkList::iterator pt = systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
		for( ; pt != systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
		{
			moduleInfoList.insert(moduleList::value_type((*pt).DeviceName, 0));
		}
	}

	typedef   map<string, int>	nicList;
	nicList	nicInfoList;

	//Build the initial list, clear NIC state 

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
			for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++ )
			{
				nicInfoList.insert(moduleList::value_type((*pt1).HostName, 0));
			}
		}
	}

	//
	// Get ext device info
	//
	SystemExtDeviceConfig systemextdeviceconfig;

	try{
		oam.getSystemConfig(systemextdeviceconfig);
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

	typedef   map<string, int>	extDeviceList;
	extDeviceList	extDeviceInfoList;

	//Build the initial list, clear ext device state 

	for ( unsigned int i = 0 ; i < systemextdeviceconfig.Count; i++)
	{
		string name = systemextdeviceconfig.extdeviceconfig[i].Name;
		extDeviceInfoList.insert(extDeviceList::value_type(name, 0));
	}

	int rtnCode = 0;
	Configuration configData;
	SystemStatus systemstatus;

	bool enableModuleMonitor = true;

	bool LANOUTAGEACTIVE = false;
	bool HOTSTANDBYACTIVE = false;
	bool downActiveOAMModule = false;

	// monitor module and external device loop

	while (true)
	{
		//don't peform module test if system is MAN_OFFLINE or not getting status's
		while(true)
		{
			SystemStatus systemstatus;
			try {
				oam.getSystemStatus(systemstatus);

				if (systemstatus.SystemOpState == oam::MAN_OFFLINE )
					sleep(5);
				else
					break;
			}
			catch(...)
			{
				sleep(5);
			}
		}

		// Module Heartbeat period and failure count
	    	int ModuleHeartbeatPeriod;
    		int ModuleHeartbeatCount;
	
		try {
			oam.getSystemConfig("ModuleHeartbeatPeriod", ModuleHeartbeatPeriod);
			oam.getSystemConfig("ModuleHeartbeatCount", ModuleHeartbeatCount);
			ModuleHeartbeatPeriod = ModuleHeartbeatPeriod * 10;
		}
		catch (exception& ex) {
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: " + error, LOG_TYPE_ERROR);
		}
		catch(...)
		{
			log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemConfig: Caught unknown exception!", LOG_TYPE_ERROR);
		}

		// skip testing if Heartbeat is disable	
		if( ModuleHeartbeatPeriod <= 0 ) {
			if ( enableModuleMonitor )
				log.writeLog(__LINE__, "ModuleHeartbeatPeriod set to disabled", LOG_TYPE_DEBUG);
			enableModuleMonitor = false;
		}
		else
		{
			if ( !enableModuleMonitor && moduleInfoList.size() > 1 )
				log.writeLog(__LINE__, "ModuleHeartbeatPeriod set to enabled", LOG_TYPE_DEBUG);
			enableModuleMonitor = true;
		}

		//single server system
		if ( moduleInfoList.size() <= 1)
 			enableModuleMonitor = false;

		// 
		// ping NIC
		//

		// read each time to catch updates
		pthread_mutex_lock(&THREAD_LOCK);
		systemModuleTypeConfig.moduletypeconfig.clear();
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
		pthread_mutex_unlock(&THREAD_LOCK);

		bool LANOUTAGESUPPORT = true;
		bool LOCALNICDOWN = false;

		if (enableModuleMonitor)
		{
			//test main local Ethernet interface status
			for ( int count = 0 ; ; count ++)
			{
				int sockfd;
				struct ifreq ifr;
			
				sockfd = socket(AF_INET, SOCK_DGRAM, 0);
				if(sockfd == -1){
					log.writeLog(__LINE__, "Could not get socket to check", LOG_TYPE_ERROR);
					close(sockfd);
					break;
				}
			
				/* get interface name */
				strncpy(ifr.ifr_name, iface_name.c_str(), IFNAMSIZ);
			
				/* Read interface flags */
				if (ioctl(sockfd, SIOCGIFFLAGS, &ifr) < 0) {
					log.writeLog(__LINE__, "Error reading ioctl", LOG_TYPE_ERROR);
					close(sockfd);
					break;
				}
			
				if (ifr.ifr_flags & IFF_UP) {
					// ethernet port is up, continue on
					close(sockfd);
					break;
				}
				else
				{
					// ethernet port is down
					log.writeLog(__LINE__, "NIC #1 is DOWN", LOG_TYPE_WARNING);
					if ( count >= ModuleHeartbeatCount ) {
						LOCALNICDOWN = true;
						close(sockfd);
						break;
					}
					else
						sleep(5);
				}
				close(sockfd);
			}
		}

		// if the NIC is down, go directly to LAN outage processing
		if ( !LOCALNICDOWN )
		{
			for ( unsigned int i = 0 ; i < systemModuleTypeConfig.moduletypeconfig.size(); i++)
			{
				int moduleCount = systemModuleTypeConfig.moduletypeconfig[i].ModuleCount;
				if( moduleCount == 0)
					continue;
	
				DeviceNetworkList::iterator pt = systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
				for( ; pt != systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
				{
					string moduleName = (*pt).DeviceName;
					string ipAddr;
					string hostName;
					int moduleState = oam::INITIAL;
					HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
					for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++ )
					{
						ipAddr = (*pt1).IPAddr;
						hostName = (*pt1).HostName;
	
						if (enableModuleMonitor)
						{
							// perform ping test
							cmd = cmdLine + ipAddr + cmdOption;
							rtnCode = system(cmd.c_str());
							rtnCode = WEXITSTATUS(rtnCode);
						}
						else
							rtnCode = 0;

						int currentNICState;
						try {
							oam.getNICStatus(hostName, currentNICState);
						}
						catch (exception& ex)
						{
							string error = ex.what();
							log.writeLog(__LINE__, "EXCEPTION ERROR on getNICStatus: " + error, LOG_TYPE_ERROR);
						}
						catch(...)
						{
							log.writeLog(__LINE__, "EXCEPTION ERROR on getNICStatus: Caught unknown exception!", LOG_TYPE_ERROR);
						}
	
						switch (rtnCode) {
							case 0:
								//NIC Ack ping
								if ( currentNICState != oam::UP ) {
									processManager.setNICState(hostName, oam::UP);
			
									if( ModuleHeartbeatPeriod > 0 )
										//Clear an alarm 				
										aManager.sendAlarmReport(hostName.c_str(), NIC_DOWN_AUTO, CLEAR);
								}
		
								//set LAN Outage indicator to false since a module is responding
								if ( moduleState == oam::INITIAL)
									if ( moduleName != config.moduleName())
										LANOUTAGESUPPORT = false;
	
								//set Module State
								if ( moduleState == oam::INITIAL || moduleState == oam::UP)
									moduleState = oam::UP;
								break;
		
							default:
								//NIC failed to respond to ping
								if ( currentNICState != oam::DOWN ) {
									log.writeLog(__LINE__, "NIC failed to respond to ping: " + hostName, LOG_TYPE_WARNING);
									processManager.setNICState(hostName, oam::DOWN);
						
									if( ModuleHeartbeatPeriod > 0 )
										//Issue an alarm 				
										aManager.sendAlarmReport(hostName.c_str(), NIC_DOWN_AUTO, SET);
								}
	
								//set Module State
								if ( moduleState == oam::INITIAL || moduleState == oam::DOWN)
									moduleState = oam::DOWN;
								else
									// NIC 1 is up and NIC 2 is down
									moduleState = oam::DEGRADED;
								break;
						}
					}
	
					// if disable, default module state to up
					if (!enableModuleMonitor)
						moduleState = oam::UP;
	
					// moduleState coming out of the NIC monitoring loop
					// UP - ALL NICs passed ping test
					// DEGRADED - NIC 1 passed, NIC 2 failed ping test
					// DOWN - NIC 1 or ALL NICs failed ping test
	
					int opState;
					try{
						bool degraded;
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
	
					// skip module check if not inuse or in FAILED state
					if (opState == oam::MAN_OFFLINE || 
						opState == oam::MAN_DISABLED ||
						opState == oam::FAILED)
						continue;
	
					//fast track a restart of a downed failover modules
					if ( gdownActiveOAMModule == moduleName ) {
						moduleInfoList[moduleName] = ModuleHeartbeatCount-1;
						gdownActiveOAMModule.clear();
						moduleState = oam::DOWN;
						downActiveOAMModule = true;
					}
	
					vector<string>::iterator pt2 = downModuleList.begin();
				
					for( ; pt2 != downModuleList.end() ; pt2++)
					{
						if ( *pt2 == moduleName ) {
							moduleInfoList[moduleName] = ModuleHeartbeatCount-1;
							moduleState = oam::DOWN;
							downModuleList.erase(pt2);
							break;
						}
					}
	
					switch (moduleState){
						case oam::DEGRADED:
							// do nothing for now
							break;
						case oam::UP:
// comment out, only come up when both nic are up, if not the pms list will not have the second nic in there
//						case oam::DEGRADED:
							if (opState == oam::DOWN || opState == oam::INITIAL
								|| opState == oam::AUTO_DISABLED)
							{
								//Set the module state to up
								processManager.setModuleState(moduleName, moduleState);
							}

							if ( moduleName == config.OAMStandbyName() )
								HOTSTANDBYACTIVE = true;
	
							// if LAN OUTAGE ACTIVE, skip module checks
							if (LANOUTAGEACTIVE)
								break;
	
							if (moduleInfoList[moduleName] >= ModuleHeartbeatCount ||
								opState == oam::DOWN || opState == oam::AUTO_DISABLED)
							{
								log.writeLog(__LINE__, "Module alive, bring it back online: " + moduleName, LOG_TYPE_DEBUG);

								string PrimaryUMModuleName = config.moduleName();
								try {
									oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);
								}
								catch(...) {}

								bool busy = false;
								for ( int retry = 0 ; retry < 20 ; retry++ )
								{
									busy = false;
									ProcessStatus DMLprocessstatus;
									try {
										oam.getProcessStatus("DMLProc", PrimaryUMModuleName, DMLprocessstatus);
									
										if ( DMLprocessstatus.ProcessOpState == oam::BUSY_INIT) {
											log.writeLog(__LINE__, "DMLProc in BUSY_INIT, skip bringing module online " + moduleName, LOG_TYPE_DEBUG);
											busy = true;
											sleep (5);
										}
										else
											break;
									}
									catch(...)
									{}
								}

								if (busy)
									break;

								processManager.setSystemState(oam::BUSY_INIT);

								processManager.reinitProcessType("cpimport");

								// halt the dbrm
								oam.dbrmctl("halt");
								log.writeLog(__LINE__, "'dbrmctl halt' done", LOG_TYPE_DEBUG);

								aManager.sendAlarmReport(moduleName.c_str(), MODULE_DOWN_AUTO, CLEAR);
				
								//send notification
								oam.sendDeviceNotification(config.moduleName(), MODULE_UP);

								//set module to enable state
								processManager.enableModule(moduleName, oam::AUTO_OFFLINE);

								int status;
	
								// if pm, move dbroots back to pm
								if ( ( moduleName.find("pm") == 0 && !amazon ) ||
									( moduleName.find("pm") == 0 && amazon && downActiveOAMModule ) ) {

									//restart to get the versionbuffer files closed so it can be unmounted
									processManager.restartProcessType("WriteEngineServer");

									downActiveOAMModule = false;
									try {
										log.writeLog(__LINE__, "Call autoUnMovePmDbroot", LOG_TYPE_DEBUG);
										oam.autoUnMovePmDbroot(moduleName);

										//check if any dbroots got assigned back to this module
										// they could not be moved if there were busy on other pms
										DBRootConfigList dbrootConfigList;
										try
										{
											int moduleID = atoi(moduleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
											oam.getPmDbrootConfig(moduleID, dbrootConfigList);
							
											if (  dbrootConfigList.size() == 0 )
											{
												// no dbroots, fail module
												log.writeLog(__LINE__, "autoUnMovePmDbroot left no dbroots mounted, failing module restart: " + moduleName, LOG_TYPE_WARNING);
		
												//Issue an alarm 				
												aManager.sendAlarmReport(moduleName.c_str(), MODULE_DOWN_AUTO, SET);
						
												//set module to disable state
												processManager.disableModule(moduleName, true);
		
												//call dbrm control
												oam.dbrmctl("reload");
												log.writeLog(__LINE__, "'dbrmctl reload' done", LOG_TYPE_DEBUG);
	
												// resume the dbrm
												oam.dbrmctl("resume");
												log.writeLog(__LINE__, "'dbrmctl resume' done", LOG_TYPE_DEBUG);

												//clear count
												moduleInfoList[moduleName] = 0;
	
												processManager.setSystemState(oam::ACTIVE);
												break;
											}
										}
										catch(...)
										{}

										log.writeLog(__LINE__, "autoUnMovePmDbroot success", LOG_TYPE_DEBUG);

										//distribute config file
										processManager.distributeConfigFile("system");	
									}
									catch(...)
									{
										log.writeLog(__LINE__, "autoUnMovePmDbroot: Failed. Fail Module", LOG_TYPE_WARNING);

										//Issue an alarm 				
										aManager.sendAlarmReport(moduleName.c_str(), MODULE_DOWN_AUTO, SET);
				
										//set module to disable state
										processManager.disableModule(moduleName, true);

										//call dbrm control
										oam.dbrmctl("reload");
										log.writeLog(__LINE__, "'dbrmctl reload' done", LOG_TYPE_DEBUG);
	
										// resume the dbrm
										oam.dbrmctl("resume");
										log.writeLog(__LINE__, "'dbrmctl resume' done", LOG_TYPE_DEBUG);

										//clear count
										moduleInfoList[moduleName] = 0;

										processManager.setSystemState(oam::ACTIVE);
										break;
									}
								}

								//restart module processes
								int retry = 0;

								int ModuleProcMonWaitCount = 30;
								try{
									oam.getSystemConfig("ModuleProcMonWaitCount", ModuleProcMonWaitCount);
								}
								catch(...) {
									ModuleProcMonWaitCount = 30;
								}

								for ( ; retry < ModuleProcMonWaitCount ; retry ++ )
								{
									// first, wait until module's ProcMon is ACTIVE
									int opState;
									try {
										ProcessStatus procstat;
										oam.getProcessStatus("ProcessMonitor", moduleName, procstat);
										opState = procstat.ProcessOpState;
							
										if (opState != oam::ACTIVE) {
											log.writeLog(__LINE__, "Waiting for Module ProcMon to go ACTIVE: " + moduleName, LOG_TYPE_DEBUG);
											sleep(5);
											continue;
										}
									}
									catch (exception& ex)
									{
										string error = ex.what();
										log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
										sleep(5);
										continue;
									}
									catch(...)
									{
										log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
										sleep(5);
										continue;
									}

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
					
										if ( ELmoduleName == moduleName )
										{	//match found assign Elastic IP Address
											string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);
											string ELIPaddress;
											try{
												oam.getSystemConfig(AmazonElasticIPAddr, ELIPaddress);
											}
											catch(...) {}
					
											try{
												oam.assignElasticIP(hostName, ELIPaddress);
												log.writeLog(__LINE__, "Set Elastic IP Address: " + hostName + "/" + ELIPaddress, LOG_TYPE_DEBUG);
											}
											catch(...) {
												log.writeLog(__LINE__, "Failed to Set Elastic IP Address: " + hostName + "/" + ELIPaddress, LOG_TYPE_ERROR);
											}
											break;
										}
									}

									// next, stopmodule to start up clean
									status = processManager.stopModule(moduleName, oam::FORCEFUL, false);
									if ( status == oam::API_SUCCESS ) {
										string newStandbyModule = processManager.getStandbyModule();
										if ( !newStandbyModule.empty() && newStandbyModule != "NONE") {
											processManager.setStandbyModule(newStandbyModule);
										}
										else
										{
											if ( newStandbyModule == "NONE")
												if ( moduleName.substr(0,MAX_MODULE_TYPE_SIZE) == "pm" )
													processManager.setStandbyModule(moduleName);
										}
									}
									else {
										//stop failed, retry
										log.writeLog(__LINE__, "stopModule, failed will retry: " + moduleName, LOG_TYPE_DEBUG);
										sleep(5);
										continue;
									}

									//call dbrm control, need to resume before start so the getdbrmfiles halt doesn't hang
									oam.dbrmctl("reload");
									log.writeLog(__LINE__, "'dbrmctl reload' done", LOG_TYPE_DEBUG);
	
									// resume the dbrm
									oam.dbrmctl("resume");
									log.writeLog(__LINE__, "'dbrmctl resume' done", LOG_TYPE_DEBUG);

									// next, startmodule
									status = processManager.startModule(moduleName, oam::FORCEFUL, oam::AUTO_OFFLINE);
									if ( status == oam::API_SUCCESS )
										break;

									log.writeLog(__LINE__, "startModule, failed will retry: " + moduleName, LOG_TYPE_DEBUG);

									//sleep and retry all over again
									sleep (5);
								} // end of the retry loop

								if ( retry < ModuleProcMonWaitCount )
								{	// module successfully started

									//distribute config file
									processManager.distributeConfigFile("system");	
									sleep(1);

									// if a PM module was started successfully, restart ACTIVE ExeMgr(s) / mysqld
									if( moduleName.find("pm") == 0 ) {
										processManager.restartProcessType("ExeMgr");
										processManager.restartProcessType("mysql");
									}

									// if a PM module was started successfully, DMLProc/DDLProc
									if( moduleName.find("pm") == 0 ) {
										processManager.restartProcessType("DDLProc");
										sleep(1);
										processManager.restartProcessType("DMLProc");
									}

									processManager.setSystemState(oam::ACTIVE);

									//clear count
									moduleInfoList[moduleName] = 0;

									//setup MySQL Replication for started modules
									log.writeLog(__LINE__, "Setup MySQL Replication for module recovering from outage", LOG_TYPE_DEBUG);
									DeviceNetworkList devicenetworklist;
									DeviceNetworkConfig devicenetworkconfig;
									devicenetworkconfig.DeviceName = moduleName;
									devicenetworklist.push_back(devicenetworkconfig);
									processManager.setMySQLReplication(devicenetworklist);
								}
								else
								{	// module failed to restart, place back in disabled state
									//Log failure, issue alarm, set moduleOpState 
									Configuration config;

									//Issue an alarm 				
									aManager.sendAlarmReport(moduleName.c_str(), MODULE_DOWN_AUTO, SET);

									// if pm, move dbroots back to pm
									if ( ( moduleName.find("pm") == 0 && !amazon ) ||
										( moduleName.find("pm") == 0 && amazon && downActiveOAMModule ) ) {
										//move dbroots to other modules
										try {
											log.writeLog(__LINE__, "Call autoMovePmDbroot", LOG_TYPE_DEBUG);
											oam.autoMovePmDbroot(moduleName);
											log.writeLog(__LINE__, "autoMovePmDbroot success", LOG_TYPE_DEBUG);
											//distribute config file
											processManager.distributeConfigFile("system");	
										}
										catch (exception& ex)
										{
											string error = ex.what();
											log.writeLog(__LINE__, "EXCEPTION ERROR on autoMovePmDbroot: " + error, LOG_TYPE_DEBUG);
										}
										catch(...)
										{
											log.writeLog(__LINE__, "EXCEPTION ERROR on autoMovePmDbroot: Caught unknown exception!", LOG_TYPE_ERROR);
										}
									}

									//set module to disable state
									processManager.disableModule(moduleName, true);

									//call dbrm control
									oam.dbrmctl("reload");
									log.writeLog(__LINE__, "'dbrmctl reload' done", LOG_TYPE_DEBUG);

									// resume the dbrm
									oam.dbrmctl("resume");
									log.writeLog(__LINE__, "'dbrmctl resume' done", LOG_TYPE_DEBUG);

									log.writeLog(__LINE__, "Module failed to auto start: " + moduleName, LOG_TYPE_CRITICAL);

									if ( amazon )
										processManager.setSystemState(oam::FAILED);
									else
										processManager.setSystemState(oam::ACTIVE);

									//clear count
									moduleInfoList[moduleName] = 0;
								}
							}

							break;
		
						case oam::DOWN:
							// if disabled or initial state, skip
							if (opState == oam::AUTO_DISABLED ||
								opState == oam::INITIAL)
								break;
	
							log.writeLog(__LINE__, "module failed to respond to pings: " + moduleName, LOG_TYPE_WARNING);
	
							//bump module ping failure counter
							moduleInfoList[moduleName]++;
			
							if ( moduleName == config.OAMStandbyName() )
								HOTSTANDBYACTIVE = false;
	
							if (moduleInfoList[moduleName] == ModuleHeartbeatCount)
							{	
								// if LAN OUTAGE ACTIVE,skip module checks
								if (LANOUTAGEACTIVE)
									break;
	
								//Log failure, issue alarm, set moduleOpState 
								Configuration config;
								log.writeLog(__LINE__, "module is down: " + moduleName, LOG_TYPE_CRITICAL);
				
								processManager.reinitProcessType("cpimport");

								// halt the dbrm
								oam.dbrmctl("halt");
								log.writeLog(__LINE__, "'dbrmctl halt' done", LOG_TYPE_DEBUG);

								processManager.setSystemState(oam::BUSY_INIT);

								string cmd = "/etc/init.d/glusterd restart > /dev/null 2>&1";
								system(cmd.c_str());

								//send notification
								oam.sendDeviceNotification(moduleName, MODULE_DOWN);

								//Issue an alarm 				
								aManager.sendAlarmReport(moduleName.c_str(), MODULE_DOWN_AUTO, SET);
			
								//mark all processes running on module auto-offline
								processManager.setProcessStates(moduleName, oam::AUTO_OFFLINE);
	
								//set module to disable state
								processManager.disableModule(moduleName, false);

								//call dbrm control
								oam.dbrmctl("reload");
								log.writeLog(__LINE__, "'dbrmctl reload' done", LOG_TYPE_DEBUG);

								// if Cloud Instance
								// state = running, then instance is rebooting, monitor for recovery
								// state = stopped, then try starting, if fail, remove/addmodule to launch new instance
								// state = terminate or nothing, remove/addmodule to launch new instance
								if ( amazon ) {

									if ( moduleName.find("um") == 0 )
									{
										// resume the dbrm
										oam.dbrmctl("resume");
										log.writeLog(__LINE__, "'dbrmctl resume' done", LOG_TYPE_DEBUG);
		
										//set recycle process
										processManager.recycleProcess(moduleName);
									}

									string currentIPAddr = oam.getEC2InstanceIpAddress(hostName);
									if (currentIPAddr == "stopped")
									{ // start instance
										log.writeLog(__LINE__, "Instance in stopped state, try starting it: " + hostName, LOG_TYPE_DEBUG);

										int retryCount = 6;		// 1 minutes
										if( moduleName.find("pm") == 0 )
										{
											if ( PMInstanceType == "m2.4xlarge" )
												retryCount = 15;		// 2.5 minutes
										}
										else
										{
											if( moduleName.find("um") == 0 )
												if ( UMInstanceType == "m2.4xlarge" )
													retryCount = 15;		// 2.5 minutes
										}

										int retry = 0;
										for (  ; retry < retryCount ; retry++ )
										{
											if ( oam.startEC2Instance(hostName) )
											{
												log.writeLog(__LINE__, "Instance started, sleep for 30 seconds to allow it to fully come up: " + hostName, LOG_TYPE_DEBUG);
	
												//delay then get new IP Address
												sleep(30);
												string currentIPAddr = oam.getEC2InstanceIpAddress(hostName);
												if (currentIPAddr == "stopped" || currentIPAddr == "terminated") {
													log.writeLog(__LINE__, "Instance failed to start (no ip-address), retry: " + hostName, LOG_TYPE_DEBUG);
												}
												else
												{
													// update the Calpont.xml with the new IP Address
													string cmd = "sed -i s/" + ipAddr + "/" + currentIPAddr + "/g " + startup::StartUp::installDir() + "/etc/Calpont.xml";
													system(cmd.c_str());
													break;
												}
											}
											else
											{
												log.writeLog(__LINE__, "Instance failed to start, retry: " + hostName, LOG_TYPE_DEBUG);

												sleep(10);
											}
										}

										if ( retry >= retryCount )
										{
											log.writeLog(__LINE__, "Instance failed to start, restart a new instance: " + hostName, LOG_TYPE_DEBUG);
											currentIPAddr = "terminated";
										}
									}

									if ( currentIPAddr == "terminated" || currentIPAddr == "running")
									{
										//check if down module was Standby OAM, if so find another one
										if ( moduleName == config.OAMStandbyName() ) {
			
											//set down module ProcessManager to AOS
											processManager.setProcessState(moduleName, "ProcessManager", oam::AUTO_OFFLINE, 0);
			
											//get another standby OAM module
											string newStandbyModule = processManager.getStandbyModule();
										
											//send message to start new Standby Process-Manager, if needed
											if ( !newStandbyModule.empty() && newStandbyModule != "NONE") {
												processManager.setStandbyModule(newStandbyModule);
											}
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

 										// remove/addmodule 
										log.writeLog(__LINE__, "Instance terminated, re-launching: " + hostName, LOG_TYPE_DEBUG);

										// if pm, get assigned dbroots and deattach EBS
										DBRootConfigList dbrootConfigList;
										int moduleID = atoi(moduleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
										if( moduleName.find("pm") == 0 ) {
											//get dbroots ids for to PM
											try
											{
												oam.getPmDbrootConfig(moduleID, dbrootConfigList);
											}
											catch (exception& e)
											{
												log.writeLog(__LINE__, "ERROR: getPmDbrootConfig error: " + moduleName, LOG_TYPE_DEBUG);
											}
										}

										DeviceNetworkList devicenetworklist;
										DeviceNetworkConfig devicenetworkconfig;
										HostConfig hostconfig;

										devicenetworkconfig.DeviceName = moduleName;
										if (cloud == "amazon-vpc")
											hostconfig.IPAddr = ipAddr;
										else
											hostconfig.IPAddr = oam::UnassignedName;

										hostconfig.HostName = oam::UnassignedName;
										hostconfig.NicID = 1;
										devicenetworkconfig.hostConfigList.push_back(hostconfig);

										devicenetworklist.push_back(devicenetworkconfig);

										bool pass = true;
										for ( int addRetry = 0 ; addRetry < 5 ; addRetry++ )
										{
											//remove module
											int ret = processManager.removeModule(devicenetworklist, false);
											if ( ret != oam::API_SUCCESS )
											{
												log.writeLog(__LINE__, "Instance failed to remove, retry: " + moduleName, LOG_TYPE_DEBUG);
											}
											else
											{
												pass = true;
												log.writeLog(__LINE__, "Instance removed, module: " + moduleName, LOG_TYPE_DEBUG);
											}

											// add module
											string password = oam::UnassignedName;
											try
											{
												oam.getSystemConfig("rpw", password);
											}
											catch(...)
											{
												password = oam::UnassignedName;
											}
	
											ret = processManager.addModule(devicenetworklist, password, false);
											if ( ret != oam::API_SUCCESS )
											{
												log.writeLog(__LINE__, "Instance failed to add, retry: " + moduleName, LOG_TYPE_CRITICAL);
												pass = false;
											}
											else
											{
												pass = true;
												log.writeLog(__LINE__, "New Instance Launched for " + moduleName, LOG_TYPE_DEBUG);

												// if pm, config and attach EBS
												if( moduleName.find("pm") == 0 && !dbrootConfigList.empty() ) {
													try
													{
														oam.setPmDbrootConfig(moduleID, dbrootConfigList);

														std::vector<std::string> dbrootList;
														DBRootConfigList::iterator pt1 = dbrootConfigList.begin();
														for( ; pt1 != dbrootConfigList.end() ; pt1++)
														{
															dbrootList.push_back(oam.itoa(*pt1));
														}

														//attach EBS
														try
														{
															oam.amazonReattach(moduleName, dbrootList, true);
															pass = true;
															break;
														}
														catch (exception& e)
														{
															log.writeLog(__LINE__, "ERROR: amazonReattach error on " + moduleName, LOG_TYPE_ERROR);
															pass = false;
														}
													}
													catch (exception& e)
													{
														log.writeLog(__LINE__, "ERROR: setPmDbrootConfig error on " + moduleName, LOG_TYPE_ERROR);
														pass = false;
													}
												}
												else
												{
													pass = true;
													break;
												}
											}

											if (pass)
												break;
										}

										if (pass)
											//Set the module state so it will be brought back up
											processManager.setModuleState(moduleName, oam::AUTO_DISABLED);
										else
										{
											//new instance failed to get added
											//remove and try auto moving dbroots to other pms
											processManager.removeModule(devicenetworklist, false);

											// if pm, move dbroots to other pms
											if( moduleName.find("pm") == 0 ) {
												try {
													log.writeLog(__LINE__, "Call autoMovePmDbroot", LOG_TYPE_DEBUG);
													oam.autoMovePmDbroot(moduleName);
													log.writeLog(__LINE__, "autoMovePmDbroot success", LOG_TYPE_DEBUG);
													//distribute config file
													processManager.distributeConfigFile("system");	
												}
												catch (exception& ex)
												{
													string error = ex.what();
													log.writeLog(__LINE__, "EXCEPTION ERROR on autoMovePmDbroot: " + error, LOG_TYPE_DEBUG);
												}
												catch(...)
												{
													log.writeLog(__LINE__, "EXCEPTION ERROR on autoMovePmDbroot: Caught unknown exception!", LOG_TYPE_ERROR);
												}
											}

											//set recycle process
											processManager.recycleProcess(moduleName);

											sleep(2);
											processManager.setSystemState(oam::ACTIVE);
										}
									}

									if ( moduleName.find("pm") == 0 )
									{
										// resume the dbrm
										oam.dbrmctl("resume");
										log.writeLog(__LINE__, "'dbrmctl resume' done", LOG_TYPE_DEBUG);
									}
								}
								else
								{  // non-amazon
									// if pm, move dbroots to other pms
									if( moduleName.find("pm") == 0 ) {
										try {
											log.writeLog(__LINE__, "Call autoMovePmDbroot", LOG_TYPE_DEBUG);
											oam.autoMovePmDbroot(moduleName);
											log.writeLog(__LINE__, "autoMovePmDbroot success", LOG_TYPE_DEBUG);
											//distribute config file
											processManager.distributeConfigFile("system");	
										}
										catch (exception& ex)
										{
											string error = ex.what();
											log.writeLog(__LINE__, "EXCEPTION ERROR on autoMovePmDbroot: " + error, LOG_TYPE_DEBUG);
										}
										catch(...)
										{
											log.writeLog(__LINE__, "EXCEPTION ERROR on autoMovePmDbroot: Caught unknown exception!", LOG_TYPE_ERROR);
										}
									}
	
									// resume the dbrm
									oam.dbrmctl("resume");
									log.writeLog(__LINE__, "'dbrmctl resume' done", LOG_TYPE_DEBUG);
	
									//set recycle process
									processManager.recycleProcess(moduleName);

									sleep(2);
									processManager.setSystemState(oam::ACTIVE);

									//check if down module was Standby OAM, if so find another one
									if ( moduleName == config.OAMStandbyName() ) {
		
										//set down module ProcessManager to AOS
										processManager.setProcessState(moduleName, "ProcessManager", oam::AUTO_OFFLINE, 0);
		
										//get another standby OAM module
										string newStandbyModule = processManager.getStandbyModule();
									
										//send message to start new Standby Process-Manager, if needed
										if ( !newStandbyModule.empty() && newStandbyModule != "NONE") {
											processManager.setStandbyModule(newStandbyModule);
										}
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
	
								//start SIMPLEX runtype processes on a SIMPLEX runtype module
								string moduletype = moduleName.substr(0,MAX_MODULE_TYPE_SIZE);
		
								try{
									oam.getSystemConfig(moduletype, moduletypeconfig);
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
		
								if ( moduletypeconfig.RunType == SIMPLEX ) {
									DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();
									for( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
									{
										string launchModuleName = (*pt).DeviceName;
										string launchModuletype = launchModuleName.substr(0,MAX_MODULE_TYPE_SIZE);
										if ( moduletype != launchModuletype )
											continue;

										//skip if active pm module (local module)
										if ( launchModuleName == config.moduleName() )
											continue;

										if( moduleName != launchModuleName ) {
											//check if module is active before starting any SIMPLEX STANDBY apps
											try{
												int launchopState;
												bool degraded;
												oam.getModuleStatus(launchModuleName, launchopState, degraded);
							
												if (launchopState != oam::ACTIVE && launchopState != oam::STANDBY ) {
													continue;
												}
											}
											catch (exception& ex)
											{
												string error = ex.what();
												log.writeLog(__LINE__, "EXCEPTION ERROR on : " + error, LOG_TYPE_ERROR);
											}
											catch(...)
											{
												log.writeLog(__LINE__, "EXCEPTION ERROR on getModuleStatus on module " + moduleName + ": Caught unknown exception!", LOG_TYPE_ERROR);
											}
	
											int status;
											log.writeLog(__LINE__, "Starting up STANDBY process on module " + launchModuleName, LOG_TYPE_DEBUG);
											for ( int j = 0 ; j < 20 ; j ++ )
											{
												status = processManager.startModule(launchModuleName, oam::FORCEFUL, oam::AUTO_OFFLINE);
												if ( status == API_SUCCESS) 
													break;
											}
											log.writeLog(__LINE__, "pingDeviceThread: ACK received from '" + launchModuleName + "' Process-Monitor, return status = " + oam.itoa(status), LOG_TYPE_DEBUG);
										}
									}
								}
							}
							break;
					}
				}
			} //end of for loop
		}

		// check and take action if LAN outage is flagged
		if (LANOUTAGESUPPORT && !LANOUTAGEACTIVE && LOCALNICDOWN)
		{
			log.writeLog(__LINE__, "LAN Failure detected", LOG_TYPE_CRITICAL);

			oam.sendDeviceNotification(config.moduleName(), START_PM_MASTER_DOWN);

			LANOUTAGEACTIVE = true;

			log.writeLog(__LINE__, "Kill any cpimport running", LOG_TYPE_INFO);
			system("pkill -9 cpimport");

			//request stop of local module		
			int status = processManager.stopModule(config.moduleName(), oam::FORCEFUL, false);
			if ( status != oam::API_SUCCESS )
				log.writeLog(__LINE__, "stopmodule failed", LOG_TYPE_ERROR);

			//stop snmptrap daemon process
			processManager.stopProcess(config.moduleName(), "SNMPTrapDaemon", oam::FORCEFUL, false);
		}
		else
		{
			if ( LANOUTAGEACTIVE && HOTSTANDBYACTIVE && !LOCALNICDOWN)
			{
//				pthread_mutex_unlock(&THREAD_LOCK);
				LANOUTAGEACTIVE = false;

				log.writeLog(__LINE__, "LAN Failure recovery");

				//check if this module still is active according to last know hot standby module
				ByteStream msg;
				ByteStream::byte requestID = GETPARENTOAMMODULE;
				msg << requestID;

				string parentOAMModule = processManager.sendMsgProcMon1( config.OAMStandbyName(), msg, requestID );
				if ( parentOAMModule == config.moduleName() || 
						parentOAMModule == "FAILED" ) {

					//send sighup to these guys incase they marked any PrimProcs offline
					processManager.reinitProcessType("ExeMgr");
					processManager.reinitProcessType("DDLProc");
					processManager.reinitProcessType("DMLProc");
				}
				else
				{
					//send message to local Process Monitor to run coldStandby
					ByteStream msg;
					ByteStream::byte requestID = OAMPARENTCOLD;
				
					msg << requestID;
				
					int returnStatus = processManager.sendMsgProcMon( config.moduleName(), msg, requestID );	
					log.writeLog(__LINE__, "sent OAM Parent Cold message to local Process-Monitor, status: " + oam.itoa(returnStatus) , LOG_TYPE_DEBUG);

					//request stop of local module		
					int status = processManager.stopModule(config.moduleName(), oam::INSTALL, false);
					if ( status != oam::API_SUCCESS )
						log.writeLog(__LINE__, "stopmodule failed", LOG_TYPE_ERROR);
				}
			}
		}

		// 
		// ping ext devices
		//

		// read each time to catch updates
		systemextdeviceconfig.extdeviceconfig.clear();
		try{
			oam.getSystemConfig(systemextdeviceconfig);
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

		for ( unsigned int i = 0 ; i < systemextdeviceconfig.Count ; i++ )
		{
			string extDeviceName = systemextdeviceconfig.extdeviceconfig[i].Name;
			string ipAddr = systemextdeviceconfig.extdeviceconfig[i].IPAddr;

			int opState;
			try{
				oam.getExtDeviceStatus(extDeviceName, opState);
			}
			catch (exception& ex)
			{
				string error = ex.what();
				log.writeLog(__LINE__, "EXCEPTION ERROR on getExtDeviceStatus: " + error, LOG_TYPE_ERROR);
			}
			catch(...)
			{
				log.writeLog(__LINE__, "EXCEPTION ERROR on getExtDeviceStatus: Caught unknown exception!", LOG_TYPE_ERROR);
			}

			cmd = cmdLine + ipAddr + cmdOption;
			rtnCode = system(cmd.c_str());

			switch (WEXITSTATUS(rtnCode)){
			case 0:
				//Switch Ack ping, Check whether alarm have been issued 
				if (extDeviceInfoList[extDeviceName] >= ModuleHeartbeatCount)
				{
					aManager.sendAlarmReport(extDeviceName.c_str(), EXT_DEVICE_DOWN_AUTO, CLEAR);

				}

				extDeviceInfoList[extDeviceName] = 0;

				if (opState != oam::ACTIVE)
				{
					//Set the switch state to active  
					processManager.setExtdeviceState(extDeviceName, oam::ACTIVE);
				}
				break;
			default:
				//extDevice failed to respond to ping
				log.writeLog(__LINE__, "extDevice failed to respond to ping: " + extDeviceName, LOG_TYPE_WARNING);
				extDeviceInfoList[extDeviceName]++;

				if (extDeviceInfoList[extDeviceName] == ModuleHeartbeatCount)
				{
					//Log failure, issue alarm, set extDeviceOpState 
					log.writeLog(__LINE__, "extDevice is down: " + extDeviceName, LOG_TYPE_CRITICAL);
	
					processManager.setExtdeviceState(extDeviceName, oam::AUTO_OFFLINE);
	
					//Issue an alarm 				
					aManager.sendAlarmReport(extDeviceName.c_str(), EXT_DEVICE_DOWN_AUTO, SET);
				}
				break;
			}
		} //end of for loop

		// double check to make sure the system status is ACTIVE if all module status's are ACTIVE
        if (dbrm.isDBRMReady())
        {
            int systemReady = dbrm.getSystemReady();    // -1 == fail, 0 == not ready, 1 == ready
            if (systemReady > 0)
            {
                bool updateActive = true;
                for ( unsigned int i = 0 ; i < systemModuleTypeConfig.moduletypeconfig.size(); i++)
                {
                    int moduleCount = systemModuleTypeConfig.moduletypeconfig[i].ModuleCount;
                    if ( moduleCount == 0)
                        continue;

                    DeviceNetworkList::iterator pt = systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
                    for ( ; pt != systemModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
                    {
                        string moduleName = (*pt).DeviceName;

                        int opState;
                        try
                        {
                            bool degraded;
                            oam.getModuleStatus(moduleName, opState, degraded);
                            if (opState == oam::ACTIVE ||
				opState == oam::DEGRADED || 
                                opState == oam::MAN_DISABLED || 
                                opState == oam::AUTO_DISABLED )
                                continue;
                            updateActive = false;
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
                }

                if (updateActive)
                {
					string PrimaryUMModuleName;
					try {
						oam.getSystemConfig("PrimaryUMModuleName", PrimaryUMModuleName);
					}
					catch(...) {}

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
		
					if (DMLprocessstatus.ProcessOpState == oam::ACTIVE) {

						//set the system status if a change has occurred
						SystemStatus systemstatus;
	
						try
						{
							oam.getSystemStatus(systemstatus);
						} 
						catch (exception& ex)
						{
							string error = ex.what();
							log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemStatus: " + error, LOG_TYPE_ERROR);
						} 
						catch (...)
						{
							log.writeLog(__LINE__, "EXCEPTION ERROR on getSystemStatus: Caught unknown exception!", LOG_TYPE_ERROR);
						}
	
						if ( systemstatus.SystemOpState != oam::ACTIVE )
						{
							processManager.setSystemState(oam::ACTIVE);
						}
					}
                }
            }
        }

        //go sleep for a bit
        int sleepTime = ModuleHeartbeatPeriod / 10;

        if (!enableModuleMonitor && systemextdeviceconfig.Count == 0)
            sleep(60);
        else
            sleep(sleepTime);

    }

	return;
}

/******************************************************************************************
* @brief      hdfsActiveAlarmsPushingThread
*
* purpose:    Push an image of ActiveAlarms to HDFS for non-OAMParentModule to view.
*
******************************************************************************************/
static void hdfsActiveAlarmsPushingThread()
{
	boost::filesystem::path filePath(ACTIVE_ALARM_FILE);
	boost::filesystem::path dirPath = filePath.parent_path();
	string dirName = boost::filesystem::canonical(dirPath).string();

	if (boost::filesystem::exists("/etc/pdsh/machines"))
	{
		string cpCmd =  "pdcp -a -x " + localHostName + " " + ACTIVE_ALARM_FILE + " " + dirName +
						" > /dev/null 2>&1";
		string rmCmd =  "pdsh -a -x " + localHostName + " rm -f " + ACTIVE_ALARM_FILE +
						" > /dev/null 2>&1";
		while(1)
		{
			if (boost::filesystem::exists(filePath))
				system(cpCmd.c_str());
			else
				system(rmCmd.c_str());

			sleep(ACTIVE_ALARMS_PUSHING_INTERVAL);
		}
	}

	return;
}


/*****************************************************************************************
* @brief	Processor Heartbeat Msg Thread
*
* purpose:	Read Heartbeat Messages from other Processes
*
*****************************************************************************************/
/*
static void heartbeatMsgThread()
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);

	//
	//waiting for request 	
	//
	ByteStream receivedMSG;
	IOSocket fIos;

	for (;;)
	{
		try
		{
			MessageQueueServer procmgr("ProcHeartbeatControl");
			for (;;)
			{
				try
				{
					fIos = procmgr.accept();
					receivedMSG = fIos.read();
		
					if (receivedMSG.length() > 0) {
						processManager.processMSG(fIos, receivedMSG);
					}
				}
				catch (exception& ex)
				{
					string error = ex.what();
					log.writeLog(__LINE__, "EXCEPTION ERROR on ProcHeartbeatControl.accept: " + error, LOG_TYPE_ERROR);
				}
				catch(...)
				{
					log.writeLog(__LINE__, "EXCEPTION ERROR on ProcHeartbeatControl.accept: Caught unknown exception!", LOG_TYPE_ERROR);
				}
		
				fIos.close();
			}
		}
        catch (exception& ex)
        {
			string error = ex.what();
			log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueServer for ProcMgr:" + error, LOG_TYPE_ERROR);
			// takes 2 - 4 minites to free sockets, sleep and retry
			sleep(60);
        }
        catch(...)
        {
			log.writeLog(__LINE__, "EXCEPTION ERROR on MessageQueueServer for ProcHeartbeatControl: Caught unknown exception!", LOG_TYPE_ERROR);
			// takes 2 - 4 minites to free sockets, sleep and retry
			sleep(60);
        }
	}

}
*/

/*****************************************************************************************
* @brief	Processor Heartbeat Thread
*
* purpose:	Check Heartbeat Messages from other Processes
*
*****************************************************************************************/
/*
static void heartbeatProcessThread()
{
	ProcessLog log;
	Configuration config;
	ProcessManager processManager(config, log);
	Oam oam;
	SNMPManager aManager;

	int processHeartbeatPeriod=60;	//default value to 60 seconds
	
	log.writeLog(__LINE__, "Thread Launched: Process Heartbeat!!!");

	while (true)
	{
		//
		// check and report on register process not sending heartbeats
		//

		// get process heartbeat period
		try {
			oam.getSystemConfig("ProcessHeartbeatPeriod", processHeartbeatPeriod);
			processHeartbeatPeriod = processHeartbeatPeriod * 60;
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

		Oam oam;
		log.writeLog(__LINE__, "Process Heartbeat check started, Heartbeat period is " + oam.itoa(processHeartbeatPeriod), LOG_TYPE_DEBUG);

		sleep(processHeartbeatPeriod);

		HeartBeatProcList::iterator list = hbproclist.begin();
		for( ; list != hbproclist.end() ; list++) 
		{
			string moduleName = (*list).ModuleName;
			string processName = (*list).ProcessName;
			int id = (*list).ID;

			// get Process state and only check if ACTIVE
			ProcessStatus procstat;
			try{
				oam.getProcessStatus(processName, moduleName, procstat);
			}
			catch (exception& ex)
			{
				string error = ex.what();
				log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: " + error, LOG_TYPE_ERROR);
				procstat.ProcessOpState = oam::MAN_OFFLINE;
			}
			catch(...)
			{
				log.writeLog(__LINE__, "EXCEPTION ERROR on getProcessStatus: Caught unknown exception!", LOG_TYPE_ERROR);
				procstat.ProcessOpState = oam::MAN_OFFLINE;
			}

			if ( procstat.ProcessOpState == oam::ACTIVE ) {
				// skip testing if Heartbeat is disable
				if( processHeartbeatPeriod != -1 ) {
//log.writeLog(__LINE__, "Heartbeat: Process being monitored: " + moduleName + " / " + processName + " / " + oam.itoa(id), LOG_TYPE_DEBUG);
					if ( !(*list).receiveFlag ) {
						// got a missing heartbeat, request a restart on the process
						log.writeLog(__LINE__, "heartbeatProcessThread: Failure from process " + moduleName + " / " + processName+ " / " + oam.itoa(id), LOG_TYPE_WARNING);
	
						oam.restartProcess(moduleName, processName, FORCEFUL, ACK_NO);
						(*list).receiveFlag = true;
						// reset all other entries for this process
						HeartBeatProcList::iterator list1 = hbproclist.begin();
						for( ; list1 != hbproclist.end() ; list1++) 
						{
							string moduleName1 = (*list1).ModuleName;
							string processName1 = (*list1).ProcessName;
							if ( moduleName == moduleName1 && processName == processName1 )
								(*list1).receiveFlag = true;
						}
					}
					else
						// reset receive heartbeat indication flag
						(*list).receiveFlag = false;
				}
				else
					// heartbeat is disabled
					(*list).receiveFlag=true;
			}
			else
			{	// registered process not active, remove from list
				hbproclist.erase(list);
				log.writeLog(__LINE__, "Removing OOS Process from Heartbeat Monitor list: " + moduleName + " / " + processName+ " / " + oam.itoa(id));
				break;
			}
		}
	} // end of while forever loop
}
*/
// vim:ts=4 sw=4:
