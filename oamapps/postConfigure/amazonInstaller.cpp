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
* $Id: amazonInstaller.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
******************************************************************************************/
/**
 * @file
 */

#include <iterator>
#include <numeric>
#include <deque>
#include <iostream>
#include <ostream>
#include <fstream>
#include <cstdlib>
#include <string>
#include <limits.h>
#include <sstream>
#include <exception>
#include <stdexcept>
#include <vector>
#include <stdio.h>
#include <ctype.h>
#include <netdb.h>
#include <sys/sysinfo.h>

#include <readline/readline.h>
#include <readline/history.h>

#include "liboamcpp.h"
#include "configcpp.h"
#include "snmpmanager.h"

using namespace std;
using namespace oam;
using namespace config;
using namespace snmpmanager;

#include "helpers.h"
using namespace installer;

//pthread_mutex_t THREAD_LOCK;

typedef struct Instance_struct
{
	std::string     instanceName;
	std::string     moduleName;
	std::string     IPaddress;
} Instance;

typedef std::vector<Instance> InstanceList;

typedef struct Volume_struct
{
	std::string     volumeName;
	std::string     deviceName;
	int     		dbrootID;
	std::string     moduleName;
} Volume;

typedef std::vector<Volume> VolumeList;

typedef struct ModuleIP_struct
{
	std::string     IPaddress;
	std::string     moduleName;
} ModuleIP;

typedef std::vector<ModuleIP> ModuleIPList;

typedef std::vector<std::string> PrivateIPList;

string getIPAddress(string instance);
void cleanupSystem(bool terminate = true);
void setSystemName();
void snmpAppCheck();
void setRootPassword();
void launchInstanceThread(ModuleIP moduleip);
int launchInstanceCount = 0;
void createVolumeThread(string module);
int createVolumeCount = 0;
int createdbrootid = 0;
int lid = 0;
int did = 1;
//unsigned getNumCores();
int numCores = 10;	// default

// devices b-e saved for ephemeral storage
string UMdeviceName = "/dev/sdf";
string PMdeviceName = "/dev/sd";
string deviceLetter[] = {"g","h","i","j","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z","end"};

Config* sysConfig = Config::makeConfig();
string SystemSection = "SystemConfig";
string InstallSection = "Installation";
string ModuleSection = "SystemModuleConfig";

string installDir = "/usr/local/Calpont";

InstanceList uminstancelist;
InstanceList pminstancelist;
VolumeList PMvolumelist;
VolumeList UMvolumelist;
string rootPassword = "Calpont1";
string AMIrootPassword = "Calpont1";
ModuleIPList elasticiplist;
ModuleIPList moduleiplist;

char* pcommand = 0;
string prompt;
string NMSIPAddress = "0.0.0.0";
string systemName = "calpont-1";
string UserModuleInstanceType = oam::UnassignedName;
string UserModuleSecurityGroup = oam::UnassignedName;
int PMvolumeSize = 100;
int UMvolumeSize = 10;
string localInstance;
bool cleanupRunning = false;
string x509Cert = oam::UnassignedName;
string x509PriKey = oam::UnassignedName;
string region = "us-east-1";
string autoTagging = "y";
string elasticIPs = oam::UnassignedName;
string systemType = "";
string subnetID = oam::UnassignedName;
string VPCStartPrivateIP = oam::UnassignedName;

bool noPrompting;
string mysqlpw = " ";

bool PMEBS = true;

int main(int argc, char *argv[])
{
    Oam oam;

	string usePMEBS;
	bool UMEBS = true;
	string useUMEBS;
	int dbrootPer = 1;
	int umNumber = 1;
	int pmNumber = 1;
	int    IserverTypeInstall;
	string singleServerInstall = "2";
	string postConfigureOutFile = "/root/postConfigure.log";
	bool postConfigureLog = false;
	bool postConfigureDebug = false;
	string TotalUmMemory = oam::UnassignedName;
	string NumBlocksPct = oam::UnassignedName;
	bool postConfigureCleanup = false;
	bool systemCleanup = false;
	bool systemStop = false;
	string existingPMInstances = oam::UnassignedName;
	string existingUMInstances = oam::UnassignedName;
	string UMprivateIPS = oam::UnassignedName;
	string PMprivateIPS = oam::UnassignedName;
	string UserModuleAutoSync = "n";
	int UserModuleSyncTime = 10;
	string instanceType;

	string amazonConfigFile = "/root/amazonConfig.xml";

   for( int i = 1; i < argc; i++ )
   {
		if( string("-h") == argv[i] || string("--help") == argv[i]) {
			cout << endl;
			cout << "This is the Amazon InfiniDB AMI System Configuration and Installation tool." << endl;
			cout << "It will Configure and startup an Amazon InfiniDB System." << endl << endl;
			cout << "It will read the system configuration settings from /root/amazonConfig.xml." << endl;
			cout << "Or user can provide a different configuration file with the -c option." << endl;
			cout << "Or if /root/amazonConfig.xml doesn't exist, then user will be prompted for settings." << endl;
			cout << endl;
   			cout << "Usage: amazonInstaller -c 'config.xml' -h -l -v -pc -d -s" << endl;
			cout << "   -h  	Help" << endl;
			cout << "   -c  	system config file, default is '/root/amazonConfig.xml'" << endl;
			cout << "   -l  	logfile for postConfigure output to /root/postConfigure.log" << endl;
			cout << "   -v  	InfiniDB version" << endl;
			cout << "   -pc	postConfigure failure System Cleanup, used to run System Cleanup if InfiniDB fails to install" << endl; 
			cout << "   -d	Delete Cluster, used to delete Instances and Volumes on a shutdowned system" << endl; 
			cout << "   	Require argument, include name of local Amazon Configure File '-c' option" << endl; 
			cout << "   -s	Stop Cluster, used to stop Instances on a shutdowned system" << endl; 
			exit (0);
		}
		else if( string("-c") == argv[i] ) {
			i++;
			if (i >= argc ) {
				cout << "   ERROR: Config File not provided" << endl;
				exit (1);
			}
			amazonConfigFile = argv[i];

			//check if file exist
			ifstream oldFile (amazonConfigFile.c_str());
			if (!oldFile) {
				cout << "ERROR: Amazon Configure File (" + amazonConfigFile + ") doesn't exist, exiting" << endl;
				exit (1);
			}
		}
		else if( string("-l") == argv[i] ) {
			postConfigureLog = true;
		}
		else if( string("-v") == argv[i] ) {
			SystemSoftware systemsoftware;
			oam.getSystemSoftware(systemsoftware);

			cout << endl << "InfiniDB Version: " << systemsoftware.Version << "-" << systemsoftware.Release << endl;
			exit (0);
		}
		else if( string("-pc") == argv[i] ) {
			postConfigureCleanup = true;
		}
		else if( string("-d") == argv[i] ) {
			systemCleanup = true;
		}
		else if( string("-s") == argv[i] ) {
			systemStop = true;
		}
		else if( string("-t") == argv[i] ) {
			i++;
			if (i >= argc ) {
				cout << "   ERROR: Thread Count not provided" << endl;
				exit (1);
			}
			string temp = argv[i];
			numCores = atoi(temp.c_str());
		}
	}

	//check if InfiniDB is up and running
	string cmd = installDir + "/bin/infinidb status > /tmp/status.log";
	system(cmd.c_str());
	if (oam.checkLogStatus("/tmp/status.log", "InfiniDB is running") ) {
		cout << endl << "InfiniDB is running, can't run AmazonInstaller while InfiniDB is running. Exiting.." << endl;
		exit (0);
	}

	if ( systemCleanup || systemStop )
	{
		//check if config file exist
		ifstream oldFile (amazonConfigFile.c_str());
		if (!oldFile) {
			cout << "Amazon Configuration File (" << amazonConfigFile << ") doesn't exist, exiting..." << endl;
			exit (1);
		}

		//read system paramaters from config file
		Config* sysConfig = Config::makeConfig();
	
		try {
			systemName = sysConfig->getConfig("SystemConfig", "SystemName");
		}
		catch(...) {}

		if ( systemCleanup )
		{
			cout << endl << "**** WARNING - Deleting '" << systemName << "' cluster - WARNING ***** " << endl << endl;
			cout << "You have selected the Option to perform a system cleanup, which will delete" << endl;
			cout << "all of the Instances (excluding the local one) and all of the associated Volumes." << endl << endl;
		}
		else
		{
			cout << endl << "**** WARNING - Stopping '" << systemName << "' cluster - WARNING ***** " << endl << endl;
			cout << "You have selected the Option to stop all the Instances in the cluster";
			cout << " (excluding the local one)." << endl << endl;
		}

		while(true) {
			string ready = "n";
			if ( systemCleanup )
				prompt = "WARNING - You are about the delete '" + systemName + "' cluster.  Are you sure you want to do this?  [y,n] (n) > ";
			else
				prompt = "WARNING - You are about the stop the instances on '" + systemName + "' cluster.  Are you sure you want to do this?  [y,n] (n) > ";
			pcommand = readline(prompt.c_str());
			if (pcommand) {
				if (strlen(pcommand) > 0) ready = pcommand;
				free(pcommand);
				pcommand = 0;
				if (ready == "n")
					exit(0);
			}

			if ( ready == "y" )
				break;

			cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
		}

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
				if ( moduleCount == 0 )
					// skip if no modules
					continue;
	
				string moduletype = systemmoduletypeconfig.moduletypeconfig[i].ModuleType;
	
				DeviceNetworkList::iterator pt = systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.begin();
				for( ; pt != systemmoduletypeconfig.moduletypeconfig[i].ModuleNetworkList.end() ; pt++)
				{
					string modulename = (*pt).DeviceName;
					string moduleID = modulename.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);

					HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
					for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
					{
						Instance instance;
				
						instance.instanceName = (*pt1).HostName;
						instance.moduleName = modulename;

						if ( moduletype == "um") {
							uminstancelist.push_back(instance);

							string UMStorageType = "internal";
							{
								try{
									oam.getSystemConfig("UMStorageType", UMStorageType);
								}
								catch(...) {}
							}
		
							if ( UMStorageType == "external" )
							{
								string volumeNameID = "UMVolumeName" + moduleID;
								string volumeName = oam::UnassignedName;

								try {
									oam.getSystemConfig( volumeNameID, volumeName);
								}
								catch(...)
								{}
							
								if ( volumeName.empty() || volumeName == oam::UnassignedName )
									break;
								else
								{
									Volume volume;
									volume.volumeName = volumeName;
									volume.moduleName = modulename;
					
									UMvolumelist.push_back(volume);
								}
							}
						}
						else
							pminstancelist.push_back(instance);
					}
	
					DeviceDBRootList::iterator pt3 = systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.begin();
					for( ; pt3 != systemmoduletypeconfig.moduletypeconfig[i].ModuleDBRootList.end() ; pt3++)
					{
						if ( (*pt3).DeviceID == atoi(moduleID.c_str()) ) {
							DBRootConfigList::iterator pt2 = (*pt3).dbrootConfigList.begin();
							for( ; pt2 != (*pt3).dbrootConfigList.end() ;)
							{
								string DBRootStorageType = "internal";
								{
									try{
										oam.getSystemConfig("DBRootStorageType", DBRootStorageType);
									}
									catch(...) {}
								}
			
								if ( DBRootStorageType == "external" )
								{
									string volumeNameID = "PMVolumeName" + oam.itoa(*pt2);
									string volumeName = oam::UnassignedName;

									try {
										oam.getSystemConfig( volumeNameID, volumeName);
									}
									catch(...)
									{}
								
									if ( volumeName.empty() || volumeName == oam::UnassignedName )
										break;
									else
									{
										Volume volume;
										volume.volumeName = volumeName;
										volume.dbrootID = *pt2;
						
										PMvolumelist.push_back(volume);
									}
								}

								pt2++;
							}
						}
					}
				}
			}
		}
		catch (exception& e)
		{
			cout << endl << "**** getModuleConfig Failed =  " << e.what() << endl;
		}

		cleanupSystem(systemCleanup);
		exit(0);
	}

	cout << endl;
	cout << "This is the Amazon InfiniDB AMI System Configuration and Installation tool." << endl;
	cout << "It will Configure and startup an Amazon InfiniDB System." << endl;

	//check if InfiniDB is up and running
	cmd = installDir + "/bin/infinidb status > /tmp/status.log";
	system(cmd.c_str());
	if (oam.checkLogStatus("/tmp/status.log", "InfiniDB is running") ) {
		cout << "InfiniDB is running, can't run amazonInstaller while InfiniDB is running. Exiting.." << endl;
		exit (0);
	}

	//backup current Calpont.xml
	string configFile = installDir + "/etc/Calpont.xml";
	string saveFile = installDir + "/etc/Calpont.xml.save";
	cmd = "rm -f " + saveFile;
	system(cmd.c_str());
	cmd = "cp " + configFile + " " + saveFile;
	system(cmd.c_str());

	//run pre-uninstall and post-install to start with a clean system
	cmd = installDir + "/bin/pre-uninstall  > /dev/null 2>&1";
	system(cmd.c_str());
	cmd = installDir + "/bin/post-uninstall  > /dev/null 2>&1";
	system(cmd.c_str());

	//backup original Calpont.xml if it doesn't exist
	//if it does exist copy to Calpont.xml

	ifstream file (saveFile.c_str());
	if (!file) {
		cmd = "cp " + configFile + " " + saveFile;
		system(cmd.c_str());
	}
	else
	{
		unlink(configFile.c_str());
		cmd = "cp " + saveFile + " " + configFile;
		system(cmd.c_str());
	}

	//check if config file exist, if not, prompt from configuration settings
	ifstream oldFile (amazonConfigFile.c_str());
	if (!oldFile) {

		cout << endl << "===== Setup Amazon files =====" << endl << endl;

		while(true) {
			cout << "For Amazon EC2 Instance installs, these files will need to be installed on" << endl;
			cout << "on the local instance:" << endl << endl;
			cout << " 1. X.509 Certificate" << endl;
			cout << " 2. X.509 Private Key" << endl << endl;
	
			while(true) {
				string ready = "y";
				prompt = "Are these files installed and ready to continue [y,n] (y) > ";
				pcommand = readline(prompt.c_str());
				if (pcommand) {
					if (strlen(pcommand) > 0) ready = pcommand;
					free(pcommand);
					pcommand = 0;
					if (ready == "n") {
						cout << endl << "Please Install these files and re-run amazonInstaller. exiting..." << endl;
						exit(0);
					}
				}
	
				if ( ready == "y" )
					break;
	
				cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
			}
	
			try {
				x509Cert = sysConfig->getConfig(InstallSection, "AmazonX509Certificate");
				x509PriKey = sysConfig->getConfig(InstallSection, "AmazonX509PrivateKey");
				region = sysConfig->getConfig(InstallSection, "AmazonRegion");
			}
			catch(...)
			{}
	
			cout << endl;
	
			while(true)
			{
				prompt = "Enter name with directory of the X.509 Certificate file (" + x509Cert + ") > ";
				pcommand = readline(prompt.c_str());
				if (pcommand) {
					if (strlen(pcommand) > 0) x509Cert = pcommand;
					free(pcommand);
					pcommand = 0;
				}
				ifstream File (x509Cert.c_str());
				if (!File)
					cout << "Error: file not found, please re-enter" << endl;
				else
					break;
			}
	
			while(true)
			{
				prompt = "Enter name with directory of the X.509 Private Key file (" + x509PriKey + ") > ";
				pcommand = readline(prompt.c_str());
				if (pcommand) {
					if (strlen(pcommand) > 0) x509PriKey = pcommand;
					free(pcommand);
					pcommand = 0;
				}
				ifstream File (x509PriKey.c_str());
				if (!File)
					cout << "Error: file not found, please re-enter" << endl;
				else
					break;
			}

			while(true)
			{
				prompt = "Enter the Amazon Region you are running in (" + region + ") > ";
				pcommand = readline(prompt.c_str());
				if (pcommand) {
					if (strlen(pcommand) > 0) region = pcommand;
					free(pcommand);
					pcommand = 0;
				}
				break;
			}

			break;
		}
	
		//set the x.509 file locations
		try {
			sysConfig->setConfig(InstallSection, "AmazonX509Certificate", x509Cert);
			sysConfig->setConfig(InstallSection, "AmazonX509PrivateKey", x509PriKey);
			sysConfig->setConfig(InstallSection, "AmazonRegion", region);
		}
		catch(...)
		{}
	
		try {
			sysConfig->write();
		}
		catch(...)
		{
			cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
			exit(1);
		}

		cout << endl << "===== Setup System Configuration =====" << endl << endl;
	
		//system Name
		prompt = "Enter System Name (" + systemName + ") > ";
		pcommand = readline(prompt.c_str());
		if (pcommand) {
			if (strlen(pcommand) > 0) systemName = pcommand;
			free(pcommand);
			pcommand = 0;
		}

		cout << endl << "There are 2 options when configuring the System Module Type: separate and combined" << endl << endl;
		cout << "  'separate' - User and Performance functionality on separate servers." << endl;
		cout << "  'combined' - User and Performance functionality on the same server" << endl << endl;

		string serverTypeInstall = "2";
		while(true)
		{
			prompt =  "Select the type of System Module Install [1=separate, 2=combined] (2) > ";
			pcommand = readline(prompt.c_str());
			cout << endl;
			if (pcommand)
			{
				if (strlen(pcommand) > 0) serverTypeInstall = pcommand;
				free(pcommand);
				pcommand = 0;
			}
	
			if ( serverTypeInstall != "1" && serverTypeInstall != "2" ) {
				cout << "Invalid Entry, please re-enter" << endl << endl;
				continue;
			}
	
			IserverTypeInstall = atoi(serverTypeInstall.c_str());
	
			break;
		}

		if (IserverTypeInstall == 1)
			systemType = "separate";
		else
			systemType = "combined";
	
		//verify and setup of modules count
		switch ( IserverTypeInstall ) {
			case (oam::INSTALL_NORMAL):
			{
				while(true)
				{
					umNumber = 1;
					prompt = "Enter number of User Modules [1,1024] (" + oam.itoa(umNumber) + ") > ";
					pcommand = readline(prompt.c_str());
					if (pcommand) {
						if (strlen(pcommand) > 0) umNumber = atoi(pcommand);
						free(pcommand);
						pcommand = 0;
					}
		
					if ( umNumber < 1 || umNumber > oam::MAX_MODULE ) {
						cout << endl << "ERROR: Invalid Module Count '" + oam.itoa(umNumber) + "', please re-enter" << endl << endl;
						continue;
					}
					break;
				}

				cout << endl;
				//get exist um imstance list
				prompt = "Enter List of Existing User Modules Instances (id1,id2,id3) or hit enter for none > ";
				pcommand = readline(prompt.c_str());
				if (pcommand) {
					if (strlen(pcommand) > 0) existingPMInstances = pcommand;
					free(pcommand);
					pcommand = 0;
				}

				instanceType = oam.getEC2LocalInstanceType();
			
				if (instanceType.empty() || instanceType == "" || instanceType == "failed")
				{
					cout << endl << "ERROR: Failed to get Instance Type, double check Configuration settings. exiting..." << endl;
					exit (1);
				}
	
				cout << endl;
				//get UserModuleInstanceType
				while(true)
				{
					UserModuleInstanceType = instanceType;
					prompt = "Enter User Module Instance Type or hit enter to default to current type (" + instanceType + ") > ";
					pcommand = readline(prompt.c_str());
					if (pcommand) {
						if (strlen(pcommand) > 0) UserModuleInstanceType = pcommand;
						free(pcommand);
						pcommand = 0;
					}
				}

				cout << endl;
				//get UserModuleSecurityGroup
				while(true)
				{
					prompt = "Enter User Module Security Group or hit enter to default to current Security Group > ";
					pcommand = readline(prompt.c_str());
					if (pcommand) {
						if (strlen(pcommand) > 0) UserModuleSecurityGroup = pcommand;
						free(pcommand);
						pcommand = 0;
					}
					break;
				}

				cout << endl;
				//get UM auto sync info
				while(true) {
					prompt = "We recommend enabling User Module Auto Syncing if using multiple User Modules, you want to enable? [y,n] (y) > ";
					pcommand = readline(prompt.c_str());
					if (pcommand) {
						if (strlen(pcommand) > 0) UserModuleAutoSync = pcommand;
						free(pcommand);
						pcommand = 0;
					}
			
					if ( UserModuleAutoSync == "n" ) {
						break;
					}
			
					if ( UserModuleAutoSync == "y") {
						while (true)
						{
							prompt = "Default sync time is 10 minutes, change if you like [1,1440] (10) > ";
							pcommand = readline(prompt.c_str());
							if (pcommand) {
								if (strlen(pcommand) > 0) UserModuleSyncTime = atoi(pcommand);
								free(pcommand);
								pcommand = 0;
							}
				
							if ( UserModuleSyncTime < 1 || UserModuleSyncTime > 1441 ) {
								cout << endl << "ERROR: Invalid Sync Time '" + oam.itoa(UserModuleSyncTime) + "', please re-enter" << endl << endl;
								continue;
							}
							break;
						}
			
						break;
					}
			
					cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
				}

				cout << endl;
				//get UMEBS volume info
				while(true) {
					string answer = "y";
					prompt = "We recommend using EBS Volumes for User Module storage, you want to use EBS Volume? [y,n] (y) > ";
					pcommand = readline(prompt.c_str());
					if (pcommand) {
						if (strlen(pcommand) > 0) answer = pcommand;
						free(pcommand);
						pcommand = 0;
					}
			
					if ( answer == "n" ) {
						UMEBS = false;
						break;
					}
			
					useUMEBS = answer;
		
					if ( answer == "y") {
						while (true)
						{
							prompt = "Default Volume size is 10gbs, change if you like [1,1024] (100) > ";
							pcommand = readline(prompt.c_str());
							if (pcommand) {
								if (strlen(pcommand) > 0) UMvolumeSize = atoi(pcommand);
								free(pcommand);
								pcommand = 0;
							}
				
							if ( UMvolumeSize < 1 || UMvolumeSize > 1024 ) {
								cout << endl << "ERROR: Invalid EBS Volume Size '" + oam.itoa(UMvolumeSize) + "', please re-enter" << endl << endl;
								continue;
							}
							break;
						}
			
						break;
					}
			
					cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
				}

				break;
			}
			default:
				break;
		}
	
		cout << endl;

		//get number of pm
		while(true)
		{
			pmNumber = 1;
			prompt = "Enter number of Performance Modules [1,1024] (" + oam.itoa(pmNumber) + ") > ";
			pcommand = readline(prompt.c_str());
			if (pcommand) {
				if (strlen(pcommand) > 0) pmNumber = atoi(pcommand);
				free(pcommand);
				pcommand = 0;
			}
	
			if ( pmNumber < 1 || pmNumber > oam::MAX_MODULE ) {
				cout << endl << "ERROR: Invalid Module Count '" + oam.itoa(pmNumber) + "', please re-enter" << endl << endl;
				continue;
			}
			break;
		}

		cout << endl;
		//get exist pm imstance list
		prompt = "Enter List of Existing Performance Modules Instances (id1,id2,id3) or hit enter for none > ";
		pcommand = readline(prompt.c_str());
		if (pcommand) {
			if (strlen(pcommand) > 0) existingPMInstances = pcommand;
			free(pcommand);
			pcommand = 0;
		}

		cout << endl;
		//get EBS Volume info
		while(true) {
			string answer = "y";
			prompt = "We recommend using EBS Volumes for Performance Module storage, you want to use EBS Volumes? [y,n] (y) > ";
			pcommand = readline(prompt.c_str());
			if (pcommand) {
				if (strlen(pcommand) > 0) answer = pcommand;
				free(pcommand);
				pcommand = 0;
			}
	
			if ( answer == "n" ) {
				PMEBS = false;
				break;
			}
	
			usePMEBS = answer;
	
			if ( answer == "y" ) {
				while (true)
				{
					cout << "The default setting is 1 EBS Volume (dbroot) per Performance Module." << endl;
					prompt = "How many EBS Volumes do you want to assign to each Performance Module (1) > ";
					pcommand = readline(prompt.c_str());
					if (pcommand) {
						if (strlen(pcommand) > 0) dbrootPer = atoi(pcommand);
						free(pcommand);
						pcommand = 0;
					}
		
					if ( dbrootPer > 0  )
						break;
					
					cout << endl << "ERROR: Invalid EBS Volume Count per pm '" + oam.itoa(dbrootPer) + "', need at least 1, please re-enter" << endl << endl;
				}
		
				while (true)
				{
					prompt = "Default Volume size is 100gbs, change if you like [1,1024] (100) > ";
					pcommand = readline(prompt.c_str());
					if (pcommand) {
						if (strlen(pcommand) > 0) PMvolumeSize = atoi(pcommand);
						free(pcommand);
						pcommand = 0;
					}
		
					if ( PMvolumeSize < 1 || PMvolumeSize > 1024 ) {
						cout << endl << "ERROR: Invalid EBS Volume Size '" + oam.itoa(PMvolumeSize) + "', please re-enter" << endl << endl;
						continue;
					}
					break;
				}
	
				break;
			}
	
			cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
		}

		cout << endl;
		//get Elasitic UP list
		prompt = "Enter List of Elastic IPs with Assigned ModuleName (x.x.x.x,um1,y.y.y.y,pm1) or hit enter for none > ";
		pcommand = readline(prompt.c_str());
		if (pcommand) {
			if (strlen(pcommand) > 0) elasticIPs = pcommand;
			free(pcommand);
			pcommand = 0;
		}

		cout << endl;

		//get auto tagging
		while(true) {
			string answer = "n";
			prompt = "Instance and Volume Name Auto tagging is enabled, do you want to disable it? [y,n] (n) > ";
			pcommand = readline(prompt.c_str());
			if (pcommand) {
				if (strlen(pcommand) > 0) answer = pcommand;
				free(pcommand);
				pcommand = 0;
			}
	
			if ( answer == "n" ) {
				autoTagging = "y";
				break;
			}
	
			if ( answer == "y" ) {
				autoTagging = "n";
				break;
			}
	
			cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
		}

		cout << endl;
		//get TotalUmMemory
		prompt = "Enter TotalUmMemory size or hit enter to default standard size > ";
		pcommand = readline(prompt.c_str());
		if (pcommand) {
			if (strlen(pcommand) > 0) TotalUmMemory = pcommand;
			free(pcommand);
			pcommand = 0;
		}

		cout << endl;
		//get NumBlocksPct
		while(true)
		{
			prompt = "Enter NumBlocksPct size or hit enter to default standard size [1,100] > ";
			pcommand = readline(prompt.c_str());
			if (pcommand) {
				if (strlen(pcommand) > 0) NumBlocksPct = pcommand;
				free(pcommand);
				pcommand = 0;
			}
			if ( ( atoi(NumBlocksPct.c_str()) < 0 || atoi(NumBlocksPct.c_str()) > 100)
					&&  NumBlocksPct != oam::UnassignedName)
				cout << "Invalid entry, please re-enter value 1-100" << endl;
			else
				break;
		}

		cout << endl;
		//check snmp Apps disable option
		snmpAppCheck();

		//get root password
		prompt = "Enter Root-Password used to access the Instances or hit enter to default to 'Calpont1' > ";
		pcommand = readline(prompt.c_str());
		if (pcommand) {
			if (strlen(pcommand) > 0) rootPassword = pcommand;
			free(pcommand);
			pcommand = 0;
		}

		cout << endl;
	}
	else
	{
		cout << "It will read the system configuration settings from " << amazonConfigFile << endl;
		cout << endl;

		//read system paramaters from config file
		Config* amazonConfig = Config::makeConfig(amazonConfigFile);
	
		try {
			x509Cert = amazonConfig->getConfig("SystemConfig", "x509CertificationFile");
		}
		catch(...)
		{}
	
		if ( x509Cert.empty() || x509Cert == oam::UnassignedName )
		{
			cout << "ERROR: x509CertificationFile not set, exiting" << endl;
			exit (1);
		}
	
		ifstream File (x509Cert.c_str());
		if (!File) {
			cout << "Error: x509CertificationFile not found, exiting" << endl;
			exit (1);
		}
	
		try {
			x509PriKey = amazonConfig->getConfig("SystemConfig", "x509PrivateFile");
		}
		catch(...)
		{}
	
		if ( x509PriKey.empty() || x509PriKey == oam::UnassignedName )
		{
			cout << "ERROR: x509PrivateFile not set, exiting" << endl;
			exit (1);
		}
	
		ifstream File1 (x509PriKey.c_str());
		if (!File1) {
			cout << "Error: x509PrivateFile not found, exiting" << endl;
			exit (1);
		}
	
		if ( x509PriKey == x509Cert ) {
			cout << "Error: x509PrivateFile and x509CertificationFile are the same file name in the config file, exiting" << endl;
			exit (1);
		}
	
		try {
			systemName = amazonConfig->getConfig("SystemConfig", "SystemName");
			systemType = amazonConfig->getConfig("SystemConfig", "SystemType");
			elasticIPs = amazonConfig->getConfig("SystemConfig", "ElasticIPs");
			umNumber = atoi(amazonConfig->getConfig("SystemConfig", "UserModuleCount").c_str());
			useUMEBS = amazonConfig->getConfig("SystemConfig", "UseUMEBSVolumes");
			UMvolumeSize = atoi(amazonConfig->getConfig("SystemConfig", "UMEBSVolumeSize").c_str());
			pmNumber = atoi(amazonConfig->getConfig("SystemConfig", "PerformanceModuleCount").c_str());
			dbrootPer = atoi(amazonConfig->getConfig("SystemConfig", "DBRootsPerPM").c_str());
			usePMEBS = amazonConfig->getConfig("SystemConfig", "UsePMEBSVolumes");
			PMvolumeSize = atoi(amazonConfig->getConfig("SystemConfig", "PMEBSVolumeSize").c_str());
			NMSIPAddress = amazonConfig->getConfig("SystemConfig", "SNMPTrapIPAddr");
			rootPassword = amazonConfig->getConfig("SystemConfig", "RootPassword");
			UserModuleInstanceType = amazonConfig->getConfig("SystemConfig", "UserModuleInstanceType");
			UserModuleSecurityGroup = amazonConfig->getConfig("SystemConfig", "UserModuleSecurityGroup");
			TotalUmMemory = amazonConfig->getConfig("SystemConfig", "TotalUmMemory");
			NumBlocksPct = amazonConfig->getConfig("SystemConfig", "NumBlocksPct");
			existingPMInstances = amazonConfig->getConfig("SystemConfig", "PerformanceModuleInstances");
			existingUMInstances = amazonConfig->getConfig("SystemConfig", "UserModuleInstances");
			UserModuleAutoSync = amazonConfig->getConfig("SystemConfig", "UserModuleAutoSync");
			UserModuleSyncTime = atoi(amazonConfig->getConfig("SystemConfig", "UserModuleSyncTime").c_str());
			autoTagging = amazonConfig->getConfig("SystemConfig", "AutoTagging");
			region = amazonConfig->getConfig("SystemConfig", "Region");
			subnetID = amazonConfig->getConfig("SystemConfig", "SubNetID");
			VPCStartPrivateIP = amazonConfig->getConfig("SystemConfig", "VPCStartPrivateIP");
			UMprivateIPS = amazonConfig->getConfig("SystemConfig", "UserModulePrivateIP");
			PMprivateIPS = amazonConfig->getConfig("SystemConfig", "PerformanceModulePrivateIP");
		}
		catch(...)
		{}
	}

	// validate config paramaters

	if ( ( atoi(NumBlocksPct.c_str()) < 0 || atoi(NumBlocksPct.c_str()) > 100)
			&&  NumBlocksPct != oam::UnassignedName) {
		cout << "Invalid NumBlocksPct (1-100), exiting..." << endl;
		exit (1);
	}

	if ( x509PriKey.find("/",0) != 0 ) {
		cout << "Error: x509PrivateFile '" + x509PriKey + "' missing directory name, exiting" << endl;
		exit (1);
	}
	
	if ( x509Cert.find("/",0) != 0 ) {
		cout << "Error: x509CertificationFile '" + x509Cert + "' missing directory name, exiting" << endl;
		exit (1);
	}

	if ( region == "eu-west-1" ||
		region == "sa-east-1" ||
		region == "us-east-1" ||
		region == "ap-northeast-1" ||
		region == "us-west-2" ||
		region == "us-west-1" ||
		region == "ap-southeast-1" )
	{
		try {
			sysConfig->setConfig(InstallSection, "AmazonRegion", region);
		}
		catch(...)
		{
			cout << endl << "ERROR: Problem setting AmazonRegion from the InfiniDB System Configuration file" << endl;
			exit(1);
		}
	}
	else
	{
		cout << endl << "ERROR: Invalid Region Name, run ec2-describe-regions to get list. exiting..." << endl;
		exit (1);
	}

	//set the x.509 file locations
	try {
		sysConfig->setConfig(InstallSection, "AmazonX509Certificate", x509Cert);
		sysConfig->setConfig(InstallSection, "AmazonX509PrivateKey", x509PriKey);
	}
	catch(...)
	{}

	try {
		sysConfig->write();
	}
	catch(...)
	{
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//get Elastic IP to module assignments
	if ( elasticIPs != oam::UnassignedName )
	{
		boost::char_separator<char> sep(",:");
		boost::tokenizer< boost::char_separator<char> > tokens(elasticIPs, sep);
		for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
				it != tokens.end();
				++it)
		{
			ModuleIP elasticip;
			elasticip.IPaddress = *it;
			++it;
			elasticip.moduleName = *it;
			elasticiplist.push_back(elasticip);
		}
	}

	//get exist UM instance list, if there is one
	vector <string> existinguminstance;
	if ( systemType == "separate")
	{
		if ( existingUMInstances != oam::UnassignedName )
		{
			boost::char_separator<char> sep(",");
			boost::tokenizer< boost::char_separator<char> > tokens(existingUMInstances, sep);
			for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
					it != tokens.end();
					++it)
			{
				existinguminstance.push_back(*it);
			}
		}
	}
	else
		umNumber = 0;

	//get exist PM instance list, if there is one
	vector <string> existingpminstance;
	if ( existingPMInstances != oam::UnassignedName )
	{
		boost::char_separator<char> sep(",");
		boost::tokenizer< boost::char_separator<char> > tokens(existingPMInstances, sep);
		for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
				it != tokens.end();
				++it)
		{
			existingpminstance.push_back(*it);
		}
	}

	//get Private IPs if VPC subNet configured
	vector <string> umprivateip;
	vector <string> pmprivateip;
	if ( subnetID != oam::UnassignedName )
	{
		//set subnetID
		try {
			sysConfig->setConfig(InstallSection, "AmazonSubNetID", subnetID);
		}
		catch(...)
		{}

		if ( VPCStartPrivateIP == "autoassign" )
		{
			for ( int um = 0 ; um < umNumber ; um++ )
			{
				umprivateip.push_back("autoassign");
			}

			for ( int pm = 0 ; pm < pmNumber ; pm++ )
			{
				pmprivateip.push_back("autoassign");
			}

			//set VPCNextPrivateIP
			try {
				sysConfig->setConfig(InstallSection, "AmazonVPCNextPrivateIP", "autoassign");
			}
			catch(...)
			{}
		}
		else
		{
			if ( VPCStartPrivateIP == oam::UnassignedName )
			{
				if ( UMprivateIPS == oam::UnassignedName )
				{
					cout << endl << "ERROR: Invalid UM Private IP Address List / VPCStartPrivateIP" << endl;
					exit(1);
				}
				else
				{
					boost::char_separator<char> sep(",");
					boost::tokenizer< boost::char_separator<char> > tokens(UMprivateIPS, sep);
					for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens.begin();
							it != tokens.end();
							++it)
					{
						if (!oam.isValidIP(*it))
						{
							cout << endl << "ERROR: Invalid UM Private IP Address: " << *it << endl;
							exit(1);
						}
		
						umprivateip.push_back(*it);
					}
	
					if ( umNumber != (int) umprivateip.size() )
					{
						cout << endl << "ERROR: Number of UMs doesn't match number of UM Private IPs Provided" << endl;
						exit(1);
					}
				}
		
				if ( PMprivateIPS == oam::UnassignedName )
				{
					cout << endl << "ERROR: Invalid PM Private IP Address List / VPCStartPrivateIP" << endl;
					exit(1);
				}
				else
				{
					boost::char_separator<char> sep(",");
					boost::tokenizer< boost::char_separator<char> > tokens1(PMprivateIPS, sep);
					for ( boost::tokenizer< boost::char_separator<char> >::iterator it = tokens1.begin();
							it != tokens1.end();
							++it)
					{
						if (!oam.isValidIP(*it))
						{
							cout << endl << "ERROR: Invalid PM Private IP Address: " << *it << endl;
							exit(1);
						}
		
						pmprivateip.push_back(*it);
					}
			
					if ( pmNumber != (int) pmprivateip.size() )
					{
						cout << endl << "ERROR: Number of PMs doesn't match number of PM Private IPs Provided" << endl;
						exit(1);
					}
				}
			}
			else
			{
				if (!oam.isValidIP(VPCStartPrivateIP))
				{
					cout << endl << "ERROR: Invalid Starting VPC Private IP Address: " << VPCStartPrivateIP << endl;
					exit(1);
				}
	
				string VPCNextPrivateIP = VPCStartPrivateIP;
	
				for ( int um = 0 ; um < umNumber ; um++ )
				{
					umprivateip.push_back(VPCNextPrivateIP);
	
					try
					{
						VPCNextPrivateIP = oam.incrementIPAddress(VPCNextPrivateIP);
					}
					catch(...)
					{
						cout << endl << "ERROR: incrementIPAddress API error, check logs" << endl;
						exit(1);
					}
				}
	
				for ( int pm = 0 ; pm < pmNumber ; pm++ )
				{
					pmprivateip.push_back(VPCNextPrivateIP);
	
					try
					{
						VPCNextPrivateIP = oam.incrementIPAddress(VPCNextPrivateIP);
					}
					catch(...)
					{
						cout << endl << "ERROR: incrementIPAddress API error, check logs" << endl;
						exit(1);
					}
				}
	
				//set VPCNextPrivateIP
				try {
					sysConfig->setConfig(InstallSection, "AmazonVPCNextPrivateIP", VPCNextPrivateIP);
				}
				catch(...)
				{}
			}
		}
	}

	try {
		sysConfig->write();
	}
	catch(...)
	{
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//get local instance name (pm1)
	localInstance = oam.getEC2LocalInstance();
	if ( localInstance == "failed" || localInstance.empty() || localInstance == "") 
	{
		cout << endl << "ERROR: Failed to get Instance ID, double check configuration setting. exiting..." << endl;
		exit (1);
	}

	instanceType = oam.getEC2LocalInstanceType();

	if (instanceType.empty() || instanceType == "" || instanceType == "failed")
	{
		cout << endl << "ERROR: Failed to get Instance Type, double check configuration setting. exiting..." << endl;
		exit (1);
	}
	
	if ( UserModuleInstanceType == oam::UnassignedName )
		UserModuleInstanceType = instanceType;

	cout << "===== Setting up system '" + systemName + "' based on these settings ===== " << endl << endl;

	SystemSoftware systemsoftware;
	oam.getSystemSoftware(systemsoftware);

	cout << "InfiniDB Version = " << systemsoftware.Version << "-" << systemsoftware.Release << endl;
	cout << "System Type = " << systemType << endl;

	if ( subnetID != oam::UnassignedName ) {
		cout << "SubNet ID = " << subnetID << endl;
		if ( VPCStartPrivateIP == oam::UnassignedName )
		{
			if ( systemType == "separate" )
				cout << "User Modules VPC Private IPs = " << UMprivateIPS << endl;

			cout << "Performance Modules VPC Private IPs = " << PMprivateIPS << endl;
		}
		else
			cout << "Starting VPC Private IP = " << VPCStartPrivateIP << endl;
	}

	if ( systemType == "separate" ) {
		cout << "Number of User Modules = " << umNumber << " (" + UserModuleInstanceType + ")" << endl;
		if ( UserModuleSecurityGroup != oam::UnassignedName )
			cout << "User Modules Instances Security Group = " << UserModuleSecurityGroup << endl;
		if ( existingUMInstances != oam::UnassignedName )
			cout << "Using User Modules Instances = " << existingUMInstances << endl;
		cout << "Using EBS Volumes for User Module storage = " << useUMEBS << endl;
		if ( useUMEBS == "y" )
			cout << "User Module EBS Volume Size = " << UMvolumeSize << endl;
	}

	cout << "Number of Performance Modules = " << pmNumber << " (" + instanceType + ")" << endl;

	if ( existingPMInstances != oam::UnassignedName )
		cout << "Using Performance Modules Instances = " << existingPMInstances << endl;

	cout << "Number of DBRoots per Performance Modules = " << dbrootPer << endl;

	if ( systemType == "separate" ) {
		cout << "Using EBS Volumes for Performance Module (DBRoot) storage = " << usePMEBS << endl;
		if ( usePMEBS == "y" )
			cout << "DBRoot EBS Volume Size = " << PMvolumeSize << endl;
	}
	else
	{
		cout << "Using EBS Volumes for Data storage = " << usePMEBS << endl;
		if ( usePMEBS == "y" )
			cout << "EBS Volume Size = " << PMvolumeSize << endl;
	}

	if ( elasticiplist.size() > 0 )
	{
		ModuleIPList::iterator list9 = elasticiplist.begin();
		for (; list9 != elasticiplist.end() ; list9++)
		{
			cout << "Elastic IP Address " << (*list9).IPaddress << " assigned to " << (*list9).moduleName << endl;
		}
	}

	cout << "SNMP Trap Receiver IP Address = " << NMSIPAddress << endl;

	if (autoTagging == "y" )
		cout << "Instance and Volume Name Auto Tagging = enabled" << endl;
	else
		cout << "Instance and Volume Name Auto Tagging = disabled" << endl;
	cout << "Amazon Region = " << region << endl;

	//set calpont config settings

	if ( TotalUmMemory != oam::UnassignedName ) {
		cout << "TotalUmMemory = " << TotalUmMemory << endl;
		try {
			sysConfig->setConfig("HashJoin", "TotalUmMemory", TotalUmMemory);
		}
		catch(...)
		{
			cout << endl << "ERROR: Problem setting SystemName from the InfiniDB System Configuration file" << endl;
			exit(1);
		}
	}

	if ( NumBlocksPct != oam::UnassignedName ) {
		cout << "NumBlocksPct = " << NumBlocksPct << endl;
		try {
			sysConfig->setConfig("DBBC", "NumBlocksPct", NumBlocksPct);
		}
		catch(...)
		{
			cout << endl << "ERROR: Problem setting SystemName from the InfiniDB System Configuration file" << endl;
			exit(1);
		}
	}

	if ( systemType == "separate" ) {
		try {
			sysConfig->setConfig(InstallSection, "UMSecurityGroup", UserModuleSecurityGroup);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting UMSecurityGroup from the InfiniDB System Configuration file" << endl;
			exit(1);
		}
		

		try {
			sysConfig->setConfig(InstallSection, "UMInstanceType", UserModuleInstanceType);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting InstanceType from the InfiniDB System Configuration file" << endl;
			exit(1);
		}

		//verify and setup of modules count
		if ( umNumber < 1 || umNumber > oam::MAX_MODULE ) {
			cout << endl << "ERROR: Invalid User Module Count '" + oam.itoa(umNumber) + "', exiting" << endl;
			exit (1);
		}
	}

	try {
		sysConfig->setConfig(InstallSection, "PMInstanceType", instanceType);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting InstanceType from the InfiniDB System Configuration file" << endl;
		exit(1);
	}
		
	if ( usePMEBS == "y" ) {
		PMEBS = true;

		if ( systemType == "combined")
			useUMEBS == "y";
	}
	else
		PMEBS = false;

	if ( useUMEBS == "y" )
		UMEBS = true;
	else
		UMEBS = false;

	try {
		 sysConfig->setConfig(SystemSection, "SystemName", systemName);
		 sysConfig->setConfig(InstallSection, "AmazonAutoTagging", autoTagging);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting SystemName from the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	if ( systemType == "separate" )
		IserverTypeInstall = 1;
	else if ( systemType == "combined" )
		IserverTypeInstall = 2;
	else {
		cout << "Error: SystemType is invalid (separare or combined only), exiting" << endl;
		exit (1);
	}
	
	// set Server Type Installation Indicator
	try {
		sysConfig->setConfig(InstallSection, "ServerTypeInstall", oam.itoa(IserverTypeInstall));
	}
	catch(...)
	{}

	// set root password
	try {
		sysConfig->setConfig(InstallSection, "rpw", rootPassword);
	}
	catch(...)
	{}

	// set rsync paramaters
	if ( UserModuleAutoSync == "y" || UserModuleAutoSync == "n" ) {
		if ( UserModuleSyncTime < 1 ) {
			cout << "Error: UserModuleSyncTime is invalid (enter a value greater than 0), exiting" << endl;
			exit (1);
		}
	}
	else
	{
		cout << "Error: UserModuleAutoSync is invalid (enter 'y' or 'n'), exiting" << endl;
		exit (1);
	}

	try {
		sysConfig->setConfig(InstallSection, "UMAutoSync", UserModuleAutoSync);
		sysConfig->setConfig(InstallSection, "UMSyncTime", oam.itoa(UserModuleSyncTime));
	}
	catch(...)
	{}

	try {
		sysConfig->write();
	}
	catch(...)
	{
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//user module
	try {
		sysConfig->setConfig(ModuleSection, "ModuleCount2", oam.itoa(umNumber));
	}
	catch(...)
	{}

	if ( pmNumber < 1 || pmNumber > oam::MAX_MODULE ) {
		cout << endl << "ERROR: Invalid Performance Module Count '" + oam.itoa(pmNumber) + "', exiting" << endl;
		exit (1);
	}

	//performance module
	try {
		sysConfig->setConfig(ModuleSection, "ModuleCount3", oam.itoa(pmNumber));
	}
	catch(...)
	{}

	if ( dbrootPer < 1  ) {
		cout << endl << "ERROR: Invalid DBRoot Count per pm '" + oam.itoa(dbrootPer) + "', exiting" << endl;
		exit (1);
	}
	
	if (PMEBS)
	{
		if ( PMvolumeSize < 1 || PMvolumeSize > 1024 ) {
			cout << endl << "ERROR: Invalid Performance Module EBS Volume Size '" + oam.itoa(PMvolumeSize) + "', exiting" << endl;
			exit (1);
		}

		try {
			sysConfig->setConfig(InstallSection, "PMVolumeSize", oam.itoa(PMvolumeSize));
		}
		catch(...)
		{}

	}

	if (UMEBS && systemType == "separate")
	{
		if ( UMvolumeSize < 1 || UMvolumeSize > 1024 ) {
			cout << endl << "ERROR: Invalid User Module EBS Volume Size '" + oam.itoa(UMvolumeSize) + "', exiting" << endl;
			exit (1);
		}

		try {
			sysConfig->setConfig(InstallSection, "UMVolumeSize", oam.itoa(UMvolumeSize));
		}
		catch(...)
		{}
	}

	try {
		sysConfig->setConfig(SystemSection, "NMSIPAddress", NMSIPAddress);
	}
	catch(...)
	{}

	try {
		sysConfig->write();
	}
	catch(...)
	{
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		cleanupSystem();
	}

	cout << endl << "===== Launch Instances =====" << endl << endl;

	Instance instance;
	instance.instanceName = localInstance;
	instance.moduleName = "pm1";
	instance.IPaddress = getIPAddress(localInstance);
	pminstancelist.push_back(instance);

	cout << "Local Instance for pm1: " + localInstance << endl;

	if ( autoTagging == "y" )
	{
		cout << " Creating Instance Tag for pm1" << endl << endl;
	
		string tagValue = systemName + "-pm1";
		oam.createEC2tag( localInstance, "Name", tagValue );
	}

	if ( systemType == "separate" )
	{
		//launch um Instances
		//check if using existing instances
		int um = 1; 
		if ( !existinguminstance.empty() )
		{
			//assign um Instances
			std::vector<std::string>::iterator list = existinguminstance.begin();
			for (; list != existinguminstance.end() ; list++)
			{
				string module = "um" + oam.itoa(um);
		
				cout << "Assigning Instance for " + module;
		
				string instanceName = *list;
				cout << ": " << instanceName << endl;
	
				//check if Instance is running	
				getIPAddress(instanceName);
	
				Instance instance;
		
				instance.instanceName = instanceName;
				instance.moduleName = module;
		
				uminstancelist.push_back(instance);
	
				um++;
				if ( um > umNumber )
					break;
			}
		}
	
		//launch um Instances
		std::vector<std::string>::iterator umprivatelist = umprivateip.begin();
		for ( ; um < umNumber+1 ; um ++ )
		{
			string module = "um" + oam.itoa(um);

			ModuleIP moduleip;
			moduleip.moduleName = module;

			if ( umprivateip.size() == 0 )
				moduleip.IPaddress = oam::UnassignedName;
			else
			{
				moduleip.IPaddress = *umprivatelist;
				umprivatelist++;
			}

			while(true)
			{
				if ( launchInstanceCount < numCores )
				{
					launchInstanceCount++;
					pthread_t launchinstancethread;
					int status = pthread_create (&launchinstancethread, NULL, (void*(*)(void*)) &launchInstanceThread, &moduleip);
					if ( status != 0 )
					{
						cout << "launchInstanceThread failed for " + module << endl;
						cleanupSystem();
					}
			
					sleep(1);
					break;
				}
				else
					sleep(10);
			}
		}
	}

	//check if using existing instances
	int pm = 2; 
	if ( !existingpminstance.empty() )
	{
		//assign pm Instances
		std::vector<std::string>::iterator list = existingpminstance.begin();
		for (; list != existingpminstance.end() ; list++)
		{
			string module = "pm" + oam.itoa(pm);
	
			cout << "Assigning Instance for " + module;
	
			string instanceName = *list;
			cout << ": " << instanceName << endl;

			//check if Instance is running	
			getIPAddress(instanceName);

			Instance instance;
	
			instance.instanceName = instanceName;
			instance.moduleName = module;
	
			pminstancelist.push_back(instance);

			pm++;
			if ( pm > pmNumber )
				break;
		}
	}

	//launch pm Instances
	std::vector<std::string>::iterator pmprivatelist = pmprivateip.begin();
	for ( ; pm < pmNumber+1 ; pm ++ )
	{
		string module = "pm" + oam.itoa(pm);

		ModuleIP moduleip;
		moduleip.moduleName = module;

		if ( pmprivateip.size() == 0 )
			moduleip.IPaddress = oam::UnassignedName;
		else
		{
			moduleip.IPaddress = *pmprivatelist;
			pmprivatelist++;
		}

		while(true)
		{
			if ( launchInstanceCount < numCores )
			{
				launchInstanceCount++;
				pthread_t launchinstancethread;
				int status = pthread_create (&launchinstancethread, NULL, (void*(*)(void*)) &launchInstanceThread, &moduleip);
				if ( status != 0 )
				{
					cout << "launchInstanceThread failed for " + module << endl;
					cleanupSystem();
				}
		
				sleep(1);
				break;
			}
			else
				sleep(10);
		}
	}

	//set is a single instance install
	bool ss = false;
	if ( systemType == "combined" && pmNumber == 1 )
		ss = true;

	if (!ss) {
		//first wait until Launch Thread Count is at zero or hit timeout
		int wait = 0;
		while (true)
		{
			if ( launchInstanceCount == 0 )
				break;

			sleep(10);
			wait++;
			// give it 30 minutes to complete
			if ( wait >= 1800 )
			{
				cout << "Timed out (30 minutes) waiting for Instances to be Launched" << endl;
				cleanupSystem();
			}
		}

		//setup new root password if needed
		if ( rootPassword != AMIrootPassword)
			setRootPassword();
	}
	else
		cout << endl;

	cout << endl;

	//setup volumes, if needed
	if ( PMEBS || UMEBS)
	{
		cout << "===== Create and Attach Volumes =====" << endl << endl;

		if (UMEBS && systemType == "separate")
		{
			//create um volumes
			for ( int vum = 1 ; vum < umNumber+1 ; vum ++ )
			{
				string module = "um" + oam.itoa(vum);

				while(true)
				{
					if ( createVolumeCount < numCores )
					{
						createVolumeCount++;
						pthread_t createvolumethread;
						int status = pthread_create (&createvolumethread, NULL, (void*(*)(void*)) &createVolumeThread, &module);
		//				createVolumeThread(module);
						if ( status != 0 )
						{
							cout << "createVolumeThread failed for " + module << endl;
							cleanupSystem();
						}
				
						sleep(5);
						break;
					}
					else
						sleep(10);
				}
			}
		}

		if ( PMEBS)
		{
			//create pm volumes
			for ( int vpm = 1 ; vpm < pmNumber+1 ; vpm ++ )
			{
				for ( int dbroot = 1 ; dbroot < dbrootPer+1 ; dbroot++ )
				{
					string module = "pm" + oam.itoa(vpm);
	
					while(true)
					{
						if ( createVolumeCount < numCores )
						{
							createVolumeCount++;
							createdbrootid++;
							pthread_t createvolumethread;
							int status = pthread_create (&createvolumethread, NULL, (void*(*)(void*)) &createVolumeThread, &module);
		//					createVolumeThread(module);
					
							if ( status != 0 )
							{
								cout << "createVolumeThread failed for " + module << endl;
								cleanupSystem();
							}
					
							sleep(5);
							break;
						}
						else
							sleep(10);
					}
				}
			}
		}

		//wait until Create Volume Count is at zero or hit timeout
		int wait = 0;
		while (true)
		{
			if ( createVolumeCount == 0 )
				break;

			sleep(10);
			wait++;
			// give it 2 hours to complete
			if ( wait >= 720 )
			{
				cout << "Timed out (120 minutes) waiting for Volumes to be created" << endl;
				cleanupSystem();
				sleep(600000);
			}
		}

		cout << endl;
	}

	cout << endl << "===== InfiniDB Configuration Setup and Installation =====" << endl << endl;

	if ( systemType == "combined" ) {
		cout << "----- Combined System Type - Setup and Install Calpont-MySQL Packages -----" << endl << endl;

		//setup external link, if needed
		if (UMEBS)
		{
			cmd = "mount " + installDir + "/data1  > /dev/null 2>&1";
			system(cmd.c_str());

			cmd = "mkdir -p " + installDir + "/data1/mysqldb  > /dev/null 2>&1";
			system(cmd.c_str());

			cmd = "mkdir -p " + installDir + "/mysql  > /dev/null 2>&1";
			system(cmd.c_str());

			cmd = "cd " + installDir + "/mysql/;ln -s " + installDir + "/data1/mysqldb db > /dev/null 2>&1";
			system(cmd.c_str());
		}

		//install rpms
		system("rpm -ivh /root/infinidb-mysql*rpm /root/infinidb-storage-engine*rpm");
		cout << endl;
	}

	// 
	// update /root/Calpont.xml
	//

	cout << "----- Updating InfiniDB Configuration File (Calpont.xml)  -----" << endl << endl;

	//setup for multi-server install
	try {
		sysConfig->setConfig(InstallSection, "SingleServerInstall", "n");
	}
	catch(...)
	{}

	//set for amazon cloud
	string cloud = "amazon-ec2";
	if ( subnetID != oam::UnassignedName )
		cloud = "amazon-vpc";

	try {
		 sysConfig->setConfig(InstallSection, "Cloud", cloud);
	}
	catch(...)
	{}

	//elastic IP setup
	if ( elasticiplist.size() > 0 )
	{
		try {
			sysConfig->setConfig(InstallSection, "AmazonElasticIPCount", oam.itoa(elasticiplist.size()));
		}
		catch(...)
		{
			cout << "ERROR: Problem setting AmazonElasticIPCount in the InfiniDB System Configuration file" << endl;
			cleanupSystem();
		}
	
		int id = 1;
		ModuleIPList::iterator list9 = elasticiplist.begin();
		for (; list9 != elasticiplist.end() ; list9++)
		{
			string moduleName = (*list9).moduleName;
			string IPaddress = (*list9).IPaddress;
	
			string AmazonElasticModule = "AmazonElasticModule" + oam.itoa(id);
			string AmazonElasticIPAddr = "AmazonElasticIPAddr" + oam.itoa(id);
	
			//write volume and device name
			try {
				sysConfig->setConfig(InstallSection, AmazonElasticModule, moduleName);
				sysConfig->setConfig(InstallSection, AmazonElasticIPAddr, IPaddress);
			}
			catch(...)
			{
				cout << "ERROR: Problem setting Volume/Device Names in the InfiniDB System Configuration file" << endl;
				cleanupSystem();
			}
	
			id++;
		}
	}

	//system dbroot config
	int systemDbrootCount = pmNumber*dbrootPer;
	try {
		sysConfig->setConfig(SystemSection, "DBRootCount", oam.itoa(systemDbrootCount));
	}
	catch(...)
	{
		cout << "ERROR: Problem setting DBRoot Count in the InfiniDB System Configuration file" << endl;
		cleanupSystem();
	}

	for ( int id = 1 ; id < systemDbrootCount+1 ; id++ )
	{
		string dbrootPar = "DBRoot" + oam.itoa(id);
		string dbrootLoc = installDir + "/data" + oam.itoa(id);
		try {
			sysConfig->setConfig(SystemSection, dbrootPar, dbrootLoc);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting DBRoot Count in the InfiniDB System Configuration file" << endl;
			cleanupSystem();
		}
	}

	//set module configuration

	InstanceList::iterator list4 = uminstancelist.begin();
	for (; list4 != uminstancelist.end() ; list4++)
	{
		string instance = (*list4).instanceName;
		string module = (*list4).moduleName;
		int moduleID = atoi(module.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
		string IPAddress = (*list4).IPaddress;
		
		string moduleHostNameParm = "ModuleHostName" + oam.itoa(moduleID) + "-1-2";
		try {
			sysConfig->setConfig(ModuleSection, moduleHostNameParm, instance);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting Host Name in the InfiniDB System Configuration file" << endl;
			cleanupSystem();
		}

		string moduleIPAddrNameParm = "ModuleIPAddr" + oam.itoa(moduleID) + "-1-2";
		try {
			sysConfig->setConfig(ModuleSection, moduleIPAddrNameParm, IPAddress);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting IP address in the InfiniDB System Configuration file" << endl;
			cleanupSystem();
		}
	}

	InstanceList::iterator list5 = pminstancelist.begin();
	for (; list5 != pminstancelist.end() ; list5++)
	{
		string instance = (*list5).instanceName;
		string module = (*list5).moduleName;
		int moduleID = atoi(module.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
		string IPAddress = (*list5).IPaddress;

		//cout << "IP Address for " << module << " " << IPAddress << endl;;
		
		string moduleHostNameParm = "ModuleHostName" + oam.itoa(moduleID) + "-1-3";
		try {
			sysConfig->setConfig(ModuleSection, moduleHostNameParm, instance);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting Host Name in the InfiniDB System Configuration file" << endl;
			cleanupSystem();
		}

		string moduleIPAddrNameParm = "ModuleIPAddr" + oam.itoa(moduleID) + "-1-3";
		try {
			sysConfig->setConfig(ModuleSection, moduleIPAddrNameParm, IPAddress);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting IP address in the InfiniDB System Configuration file" << endl;
			cleanupSystem();
		}

		//DBRoots
		string moduledbrootcount = "ModuleDBRootCount" + oam.itoa(moduleID) + "-3";
		try {
			sysConfig->setConfig(ModuleSection, moduledbrootcount, oam.itoa(dbrootPer));
		}
		catch(...)
		{
			cout << "ERROR: Problem setting dbroot count in the InfiniDB System Configuration file" << endl;
			cleanupSystem();
		}

		for ( int id = 1 ; id < dbrootPer+1 ; id++ )
		{
			string moduledbrootid = "ModuleDBRootID" + oam.itoa(moduleID) + "-" + oam.itoa(id) + "-3";

			int dbrootid;
			if ( dbrootPer == 1 )
				dbrootid = moduleID;
			else
				dbrootid = ((moduleID-1) * dbrootPer) + id;

			//cout << oam.itoa(moduleID) + " " + oam.itoa(id) + " " + oam.itoa(dbrootid) << endl;

			try {
				sysConfig->setConfig(ModuleSection, moduledbrootid, oam.itoa(dbrootid));
			}
			catch(...)
			{
				cout << "ERROR: Problem setting DBRoot ID in the InfiniDB System Configuration file" << endl;
				cleanupSystem();
			}
		}
	}

	//Config storage

	if (UMEBS && systemType == "separate")
	{
		try {
			sysConfig->setConfig(InstallSection, "UMStorageType", "external");
		}
		catch(...)
		{
			cout << "ERROR: Problem setting UMStorageType in the InfiniDB System Configuration file" << endl;
			cleanupSystem();
		}

		int id = 1;
		VolumeList::iterator list6 = UMvolumelist.begin();
		for (; list6 != UMvolumelist.end() ; list6++)
		{
			string volumeName = (*list6).volumeName;
			string deviceName = (*list6).deviceName;
	
			string volumeNameID = "UMVolumeName" + oam.itoa(id);
			string deviceNameID = "UMVolumeDeviceName" + oam.itoa(id);
	
			//write volume and device name
			try {
				sysConfig->setConfig(InstallSection, volumeNameID, volumeName);
				sysConfig->setConfig(InstallSection, deviceNameID, deviceName);
			}
			catch(...)
			{
				cout << "ERROR: Problem setting Volume/Device Names in the InfiniDB System Configuration file" << endl;
				cleanupSystem();
			}

			id++;
		}
	}

	if (PMEBS)
	{
		try {
			sysConfig->setConfig(InstallSection, "DBRootStorageType", "external");
		}
		catch(...)
		{
			cout << "ERROR: Problem setting DBRootStorageType in the InfiniDB System Configuration file" << endl;
			cleanupSystem();
		}

		cmd = "mkdir -p " + installDir + "/local/etc/pm1 > /dev/null 2>&1";
		system(cmd.c_str());

		cmd = installDir + "/local/etc/pm1/fstab";
		unlink(cmd.c_str());

		cmd = "touch " + installDir + "/local/etc/pm1/fstab";
		system(cmd.c_str());

		cmd = "/etc/fstab.installerBackup";
		unlink(cmd.c_str());

		system("cp /etc/fstab /etc/fstab.installerBackup > /dev/null 2>&1");

		int id = 1;
		VolumeList::iterator list6 = PMvolumelist.begin();
		for (; list6 != PMvolumelist.end() ; list6++)
		{
			string volumeName = (*list6).volumeName;
			string deviceName = (*list6).deviceName;
	
			string volumeNameID = "PMVolumeName" + oam.itoa(id);
			string deviceNameID = "PMVolumeDeviceName" + oam.itoa(id);
	
			//write volume and device name
			try {
				sysConfig->setConfig(InstallSection, volumeNameID, volumeName);
				sysConfig->setConfig(InstallSection, deviceNameID, deviceName);
			}
			catch(...)
			{
				cout << "ERROR: Problem setting Volume/Device Names in the InfiniDB System Configuration file" << endl;
				cleanupSystem();
			}

			//update /etc/fstab with mount
			string entry = deviceName + " " + installDir + "/data" + oam.itoa((*list6).dbrootID) + " ext2 noatime,nodiratime,noauto 0 0";
			string cmd = "echo " + entry + " >> /etc/fstab";
			system(cmd.c_str());

			//use from addmodule later
			cmd = "echo " + entry + " >> " + installDir + "/local/etc/pm1/fstab";
			system(cmd.c_str());

			id++;
		}
	}

	try {
		sysConfig->write();
	}
	catch(...)
	{
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		cleanupSystem();
	}

	//copy Calpont.xml to Calpont.xml.rpmsave for postConfigure no-prompt option
	cmd = "rm -f " + installDir + "/etc/Calpont.xml.rpmsave";
	system(cmd.c_str());
	cmd = "cp " + installDir + "/etc/Calpont.xml " + installDir + "/etc/Calpont.xml.rpmsave";
	int rtnCode = system(cmd.c_str());
	if (WEXITSTATUS(rtnCode) != 0) {
		cout << "Error copying Calpont.xml to Calpont.xml.rpmsave" << endl;
		cleanupSystem();
	}

	// run postConfigure with no-prompt option

	cout << "----- Running the System Installer Script (postConfigure) -----" << endl;

	string postConfigureCMD = installDir + "/bin/postConfigure -n -p " + rootPassword;
	if ( postConfigureDebug )
		postConfigureCMD = postConfigureCMD + " -d";

	cout << endl << " Running command: " << postConfigureCMD << endl;

	if ( postConfigureLog ) {
		cout << "Outputting to log file " + postConfigureOutFile << ", please wait..." << endl;
		string cmd = postConfigureCMD + " > " + postConfigureOutFile;
		int ret = system(cmd.c_str());
		if (WEXITSTATUS(ret) == 0 )
			cout << "postConfigure Successfully Completed, system is ready to use" << endl << endl;
		else {
			cout << "ERROR : postConfigure install error, check " <<  postConfigureOutFile << endl << endl;

			if (postConfigureCleanup) {
				cout << "Running calpont-support script is capture system logs and other data" << endl << endl; 

				string cmd = installDir + "/bin/calpontSupport -p Calpont1 -hw -s -c -r -l";
				system (cmd.c_str());
				cleanupSystem();
			}
		}
	}
	else
	{
		int ret = system(postConfigureCMD.c_str());
		if (WEXITSTATUS(ret) == 0 )
			cout << "postConfigure Successfully Completed, system is ready to use" << endl << endl;
		else {
			cout << endl << "ERROR : postConfigure install failure" << endl << endl;

			if (postConfigureCleanup) {
				cout << "Running calpont-support script is capture system logs and other data" << endl << endl; 

				string cmd = installDir + "/bin/calpontSupport -p Calpont1 -hw -s -c -r -l";
				system (cmd.c_str());
				cleanupSystem();
			}
		}
	}

}

string getIPAddress(string instance)
{
	Oam oam;

	string IPAddress = oam.getEC2InstanceIpAddress(instance);
	if (IPAddress == "stopped" || IPAddress == "terminated") {
		cout << "ERROR: Instance " + instance + " not running, couldn't get IP Address" << endl << endl;
		cleanupSystem();
	}

	return IPAddress;
}

void snmpAppCheck()
{
	Oam oam;

	cout << endl << "===== Setup the Network Management System (NMS) Server Configuration =====" << endl << endl;

	cout << "This would be used to receive SNMP Traps from InfiniDB, like a Network Control Center" << endl;
	cout << "Default to 0.0.0.0 to not enable snmptrap forwarding" << endl << endl;
	prompt = "Enter IP Address(es) of NMS Server (0.0.0.0) > ";
	pcommand = readline(prompt.c_str());
	if (pcommand)
	{
		if (strlen(pcommand) > 0) NMSIPAddress = pcommand;
		free(pcommand);
		pcommand = 0;
	}

	return;
}

void setSystemName()
{

	prompt = "Enter System Name (" + systemName + ") > ";
	pcommand = readline(prompt.c_str());
	if (pcommand)
	{
		if (strlen(pcommand) > 0) systemName = pcommand;
		free(pcommand);
		pcommand = 0;
	}
}

void setRootPassword()
{
	Oam oam;

	cout << endl << "--- Updating Root Password on all Instance(s) ---" << endl << endl;

	InstanceList::iterator list1 = uminstancelist.begin();
	for (; list1 != uminstancelist.end() ; list1++)
	{
		string instance = (*list1).instanceName;
		string module = (*list1).moduleName;

		//get IP Address of um instance
		string ipAddress = oam.getEC2InstanceIpAddress(instance);

		if (ipAddress == "stopped" || ipAddress == "terminated" ) {
			cout << "ERROR: Instance " << instance << " failed to get private IP Address" << endl;
			cleanupSystem();
		}

		string cmd = installDir + "/bin/remote_command.sh " + ipAddress + " " + AMIrootPassword + "  '/root/updatePassword.sh " + rootPassword + "' > /dev/null 2>&1";
		int rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) != 0) {
			cout << "ERROR: failed update of root password on " + module << endl;
			cleanupSystem();
		}
	}

	InstanceList::iterator list2 = pminstancelist.begin();
	for (; list2 != pminstancelist.end() ; list2++)
	{
		string instance = (*list2).instanceName;
		string module = (*list2).moduleName;

		if ( module == "pm1" ) {
			string cmd = "/root/updatePassword.sh " + rootPassword + " > /dev/null 2>&1";
			int rtnCode = system(cmd.c_str());
			if (WEXITSTATUS(rtnCode) != 0) {
				cout << "ERROR: failed update root of password on " + module << endl;
				cleanupSystem();
			}
			continue;
		}

		//get IP Address of pm instance
		string ipAddress = oam.getEC2InstanceIpAddress(instance);

		if (ipAddress == "stopped" || ipAddress == "terminated" ) {
			cout << "ERROR: Instance " << instance << " failed to get private IP Address" << endl;
			cleanupSystem();
		}

		string cmd = installDir + "/bin/remote_command.sh " + ipAddress + " " + AMIrootPassword + "  '/root/updatePassword.sh " + rootPassword + "' > /dev/null 2>&1";
		int rtnCode = system(cmd.c_str());
		if (WEXITSTATUS(rtnCode) != 0) {
			cout << "ERROR: failed update root of password on " + module << endl;
			cleanupSystem();
		}
	}
}

void launchInstanceThread(ModuleIP moduleip)
{
	Oam oam;

	//get module info
	string moduleName = moduleip.moduleName;
	string IPAddress = moduleip.IPaddress;

	//due to bad instances getting launched causing scp failures
	//have retry login around fail scp command where a instance will be relaunched
	int instanceRetry = 0;
	for ( ; instanceRetry < 5 ; instanceRetry ++ )
	{
		if (cleanupRunning)
			pthread_exit(0);

		string instanceName;
		if ( moduleName.find("um") == 0 )
			instanceName = oam.launchEC2Instance(moduleName, IPAddress, UserModuleInstanceType, UserModuleSecurityGroup);
		else
			instanceName = oam.launchEC2Instance(moduleName, IPAddress);
	
		if ( instanceName == "failed" ) {
			cout << " *** Failed to Launch an Instance for " + moduleName << ", will retry up to 5 times" << endl;
			continue;
		}
	
		cout << "Launched Instance for " << moduleName << ": " << instanceName << endl;
	
		//give time for instance to startup
		sleep(60);
	
		cout << " SCP x.509 files to " + moduleName << endl;
	
		string ipAddress = oam::UnassignedName;
	
		bool pass = false;
		for ( int retry = 0 ; retry < 60 ; retry++ )
		{
			if (cleanupRunning)
				pthread_exit(0);

			//get IP Address of pm instance
			if ( ipAddress == oam::UnassignedName || ipAddress == "stopped" || ipAddress == "No-IPAddress" )
			{
				ipAddress = oam.getEC2InstanceIpAddress(instanceName);
			
				if (ipAddress == "stopped" || ipAddress == "No-IPAddress" ) {
					sleep(5);
					continue;
				}
			}
	
			string cmd = installDir + "/bin/remote_scp_put.sh " + ipAddress + " " + AMIrootPassword + " "  + x509Cert + " > /tmp/scp.log_" + instanceName;
			int rtnCode = system(cmd.c_str());
			if (WEXITSTATUS(rtnCode) == 0) {
				pass = true;
				break;
			}
			sleep(5);
		}
	
		if (!pass)
		{
			cout << " *** SCP Failed to Instance for " + moduleName << ", recreate a new instance" << endl;
			cout << " *** Terminating Module (" + moduleName + ") Instance: " + instanceName << endl;
			oam.terminateEC2Instance( instanceName );
			continue;
		}
	
		pass = false;
		for ( int retry = 0 ; retry < 60 ; retry++ )
		{
			if (cleanupRunning)
				pthread_exit(0);

			string cmd = installDir + "/bin/remote_scp_put.sh " + ipAddress + " " + AMIrootPassword + " "  + x509PriKey + " > /tmp/scp.log_" + instanceName;
			int rtnCode = system(cmd.c_str());
			if (WEXITSTATUS(rtnCode) == 0) {
				pass = true;
				break;
			}
			sleep(5);
		}
	
		if (!pass)
		{
			cout << " *** SCP Failed to Instance for " + moduleName << ", recreate a new instance" << endl;
			cout << " *** Terminating Module (" + moduleName + ") Instance: " + instanceName << endl;
			oam.terminateEC2Instance( instanceName );
			continue;
		}

		Instance instance;
	
		instance.instanceName = instanceName;
		instance.moduleName = moduleName;
		instance.IPaddress = ipAddress;
		//cout << "IP Address for " << moduleName << " " << ipAddress << endl;

		if ( moduleName.find("um") == 0 )
			uminstancelist.push_back(instance);
		else
			pminstancelist.push_back(instance);

		if ( autoTagging == "y" )
		{
			cout << " Creating Instance Tag for " << moduleName << endl;
		
			string tagValue = systemName + "-" + moduleName;
			oam.createEC2tag( instanceName, "Name", tagValue );
		}

		if ( elasticiplist.size() > 0 )
		{
			ModuleIPList::iterator list9 = elasticiplist.begin();
			for (; list9 != elasticiplist.end() ; list9++)
			{
				if ( moduleName == (*list9).moduleName )
				{
					cout << " Assigning Elastic IP Address " << (*list9).IPaddress << " to " << (*list9).moduleName << endl;
					try{
						oam.assignElasticIP(instanceName, (*list9).IPaddress);
					}
					catch(...) {
						cout << endl << "**** Assigning Elastic IP Address Failed : assignElasticIP API Error";
					}

					break;
				}
			}
		}

		break;
	}

	if ( instanceRetry >= 5 )
	{
		cout << " *** Failed to Successfully Launch Instance for " + moduleName << endl;
		cleanupSystem();
		pthread_exit(0);
	}

	launchInstanceCount--;

	// exit thread
//	return;
	pthread_exit(0);
}


void createVolumeThread(string module)
{
	Oam oam;
	Volume volume;

	//local copies
	string moduleName = module;

	int volumeSize;
	string device;
	string volumeName;
	int dbrootid = 0;
	string name;

	if ( moduleName.find("um") == 0 ) {
		device = UMdeviceName + moduleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);

		volumeSize = UMvolumeSize;

		cout << "Creating Volume for module " << moduleName << endl;

		name = moduleName;
	}
	else
	{	//pm volume setup
		dbrootid = createdbrootid;

		device = PMdeviceName + deviceLetter[lid] + oam.itoa(did);

		volumeSize = PMvolumeSize;

		//bump id numbers
		did++;
		if ( did > 10 ) {
			did = 1;
			lid++;
			if ( deviceLetter[lid] == "end" )
				lid = 0;
		}

		cout << "Creating Volume for DBRoot#" << dbrootid << " on module " << moduleName << endl;

		name = "DBRoot#" + oam.itoa(dbrootid);
	}

	//create volume
	int retry = 0;
	for ( ; retry < 5 ; retry++ )
	{
		if (cleanupRunning)
			pthread_exit(0);

		volumeName = oam.createEC2Volume(oam.itoa(volumeSize), name);
	
		if ( volumeName == "failed" || volumeName.empty() )
			cout << " *** ERROR: Failed to create a Volume for " << name << ", retry up to 5 times " << endl;
		else
			break;
	}

	if ( retry >= 5 )
	{
		cout << " *** ERROR: Failed to create a Volume for " << name << ", do cleanup" << endl;
		cleanupSystem();
//		exit (1);
		pthread_exit(0);
	}

//	pthread_mutex_lock(&THREAD_LOCK);
	volume.moduleName = moduleName;
	volume.volumeName = volumeName;
	volume.deviceName = device;
	volume.dbrootID = dbrootid;

	if ( moduleName.find("um") == 0 )
		UMvolumelist.push_back(volume);
	else
		PMvolumelist.push_back(volume);
//	pthread_mutex_unlock(&THREAD_LOCK);

	cout << " Attach Volume and format : " << volumeName << ":" << device << " for " << name << endl;

	//attach volumes to local instance
	retry = 0;
	for ( ; retry < 5 ; retry++ )
	{
		if (cleanupRunning)
			pthread_exit(0);

		if (!oam.attachEC2Volume(volumeName, device, localInstance)) {
			cout << " *** ERROR: Volume " << volumeName << " failed to attach to: " << localInstance << ":" << name << ", retry up to 5 times " << endl;
			oam.detachEC2Volume(volumeName);
		}
		else
			break;
	}

	if ( retry >= 5 )
	{
		cout << " *** ERROR: Volume " << volumeName << " failed to attach to: " << localInstance << ":" << name << ", do cleanup " << endl;
		cleanupSystem();
//		exit (1);
		pthread_exit(0);
	}

	//format attached volume
	string cmd = "mkfs.ext2 -F " + device + " > /dev/null 2>&1";
	system(cmd.c_str());

	if (cleanupRunning)
		pthread_exit(0);

	//move volume if not assigned to local module (pm1)
	if ( moduleName != "pm1" )
	{
		retry = 0;
		for ( ; retry < 5 ; retry++ )
		{
			if (cleanupRunning)
				pthread_exit(0);

			if (!oam.detachEC2Volume(volumeName)) {
				cout << "*** ERROR: Volume " << volumeName << " failed to detach from local instance, retry up to 5 times " << endl;
			}
			else
				break;
		}

		if ( retry >= 5 )
		{
			cout << "*** ERROR: Volume " << volumeName << " failed to detach from local instance, do cleanup " << endl;
			cleanupSystem();
	//		exit (1);
			pthread_exit(0);
		}

		//attach to real instance
		if ( moduleName.find("um") == 0 )
		{
			InstanceList::iterator list2 = uminstancelist.begin();
			for (; list2 != uminstancelist.end() ; list2++)
			{
				if (cleanupRunning)
					pthread_exit(0);

				if ( moduleName == (*list2).moduleName ) {
					//attach volumes to real instance
					if (!oam.attachEC2Volume(volumeName, device, (*list2).instanceName)) {
						cout << "ERROR: Volume " << volumeName << " failed to attach to: " << (*list2).instanceName << ":" <<  moduleName << endl;
						cleanupSystem();
	//					exit (1);
						pthread_exit(0);
					}
				}
			}
		}
		else
		{
			//attach to real instance
			InstanceList::iterator list2 = pminstancelist.begin();
			for (; list2 != pminstancelist.end() ; list2++)
			{
				if (cleanupRunning)
					pthread_exit(0);

				if ( moduleName == (*list2).moduleName ) {
					//attach volumes to real instance
					retry = 0;
					for ( ; retry < 5 ; retry++ )
					{
						if (!oam.attachEC2Volume(volumeName, device, (*list2).instanceName)) {
							cout << "*** ERROR: Volume " << volumeName << " failed to attach to: " << (*list2).instanceName << ":" << moduleName << ", retry up to 5 times " << endl;
							oam.detachEC2Volume(volumeName);
						}
						else
							break;
					}
			
					if ( retry >= 5 )
					{
						cout << "*** ERROR: Volume " << volumeName << " failed to attach to: " << (*list2).instanceName << ":" << moduleName << ", do cleanup " << endl;
						cleanupSystem();
				//		exit (1);
						pthread_exit(0);
					}
				}
			}
		}
	}

	if ( autoTagging == "y" )
	{
		if ( moduleName.find("um") == 0 ) 
		{
			cout << " Creating Volume Tag for " << moduleName << endl;
		
			string tagValue = systemName + "-" + moduleName;
			oam.createEC2tag( volumeName, "Name", tagValue );
		}
		else
		{
			cout << " Creating Volume Tag for DBRoot#" << dbrootid << endl;
		
			string tagValue = systemName + "-dbroot" + oam.itoa(dbrootid);
			oam.createEC2tag( volumeName, "Name", tagValue );
		}
	}

	createVolumeCount--;

	// exit thread
//	return;
	pthread_exit(0);
}

/*
unsigned getNumCores()
{
	ifstream cpuinfo("/proc/cpuinfo");

	if (!cpuinfo.good())
		return 2;

	unsigned nc = 0;

	string line;

	getline(cpuinfo, line);

	unsigned i = 0;
	while (i < 10000 && cpuinfo.good() && !cpuinfo.eof())
	{
		string::size_type pos = line.find("processor",0);
		if (pos != string::npos)
			nc++;

		getline(cpuinfo, line);

		++i;
	}

	return nc;
}
*/
void cleanupSystem(bool terminate)
{
	Oam oam;

	if (cleanupRunning)
		return;

	cleanupRunning = true;

	//get local module name
	oamModuleInfo_t t;
	string localModuleName = "pm1";

	try {
		t = oam.getModuleInfo();
		localModuleName = boost::get<0>(t);
	}
	catch (...) {}

	if ( terminate ) {
		cout << endl << "***** Performing System Cleanup *****" << endl << endl;

		system("umount /usr/local/Calpont/data* > /dev/null 2>&1");
	
		//run pre-uninstall
		string cmd = installDir + "/bin/pre-uninstall  > /dev/null 2>&1";
		system(cmd.c_str());
	
		InstanceList::iterator list1 = uminstancelist.begin();
		for (; list1 != uminstancelist.end() ; list1++)
		{
			string instance = (*list1).instanceName;
			string module = (*list1).moduleName;
	
			cout << "Terminating User Module (" + module + ") Instance: " + instance << endl;
	
			oam.terminateEC2Instance( instance );
		}

		cout << endl;
		InstanceList::iterator list2 = pminstancelist.begin();
		for (; list2 != pminstancelist.end() ; list2++)
		{
			string instance = (*list2).instanceName;
			string module = (*list2).moduleName;
	
			if ( module == localModuleName )
				continue;
	
			cout << "Terminating Performance Module (" + module + ") Instance: " + instance << endl;
	
			oam.terminateEC2Instance( instance );
		}
	}
	else
	{
		cout << endl << "***** Performing Stopping System Instances *****" << endl << endl;
	
		InstanceList::iterator list1 = uminstancelist.begin();
		for (; list1 != uminstancelist.end() ; list1++)
		{
			string instance = (*list1).instanceName;
			string module = (*list1).moduleName;
	
			cout << "Stopping User Module (" + module + ") Instance: " + instance << endl;
	
			oam.stopEC2Instance( instance );
		}

		cout << endl;
		InstanceList::iterator list2 = pminstancelist.begin();
		for (; list2 != pminstancelist.end() ; list2++)
		{
			string instance = (*list2).instanceName;
			string module = (*list2).moduleName;
	
			if ( module == localModuleName )
				continue;
	
			cout << "Stopping Performance Module (" + module + ") Instance: " + instance << endl;
	
			oam.stopEC2Instance( instance );
		}

		cout << endl << "Instance Stoppage finished, exiting" << endl << endl;
		exit (1);
	}

	// volumes

	cout << endl;
	VolumeList::iterator list3 = UMvolumelist.begin();
	for (; list3 != UMvolumelist.end() ; list3++)
	{
		string volumeName = (*list3).volumeName;
		string module = (*list3).moduleName;

		cout << "Detaching and Deleting Volume (" + module + "): " + volumeName << endl;

		oam.detachEC2Volume( volumeName );
		oam.deleteEC2Volume( volumeName );
	}

	cout << endl;
	VolumeList::iterator list4 = PMvolumelist.begin();
	for (; list4 != PMvolumelist.end() ; list4++)
	{
		string volumeName = (*list4).volumeName;
		int dbrootID = (*list4).dbrootID;
		string device = (*list4).deviceName;

		cout << "Detaching and Deleting (DBRoot#" + oam.itoa(dbrootID) + ") Volume: " + volumeName << endl;

		//unmount pm1 disk
		if ( (*list4).moduleName == "pm1" )
		{
			string cmd = "umount " + installDir + "/data" + oam.itoa(dbrootID) + " > /dev/null 2>&1";
			system(cmd.c_str());

			cmd = "rm -f " + device + " > /dev/null 2>&1";
			system(cmd.c_str());
		}

		oam.detachEC2Volume( volumeName );
		oam.deleteEC2Volume( volumeName );
	}

	system("rm -f /root/.ssh/known_hosts");

	system("mv -f /etc/fstab.installerBackup /etc/fstab > /dev/null 2>&1");

	if ( systemType == "combined" ) {
		cout << "----- Combined System Type Uninstall Calpont-MySQL Packages -----" << endl << endl;

		system("rpm -e infinidb-mysql infinidb-storage-engine");
	}

	cout << endl << "Cleanup finished, exiting" << endl << endl;
	exit (1);

}
