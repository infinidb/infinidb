/******************************************************************************************
* $Id: postConfigure.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
* List of files being updated by post-install configure:
*		Calpont/etc/snmpd.conf
*		Calpont/etc/snmptrapd.conf
*		Calpont/etc/Calpont.xml
*		Calpont/etc/ProcessConfig.xml
*		/etc/exports 
*		/etc/fstab
*		/etc/rc.local
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

typedef std::vector<string> Devices;

typedef struct Child_Module_struct
{
	std::string     moduleName;
	std::string     moduleIP;
	std::string     hostName;
} ChildModule;

typedef std::vector<ChildModule> ChildModuleList;

typedef struct Performance_Module_struct
{
	std::string     moduleIP1;
	std::string     moduleIP2;
	std::string     moduleIP3;
	std::string     moduleIP4;
} PerformanceModule;

typedef std::vector<PerformanceModule> PerformanceModuleList;

void snmpAppCheck();
void offLineAppCheck();
bool setOSFiles(string parentOAMModuleName, int serverTypeInstall, string sharedNothing, string DBRootStorageType);
bool checkSaveConfigFile();
string getModuleName();
bool setModuleName(string moduleName);
bool updateSNMPD(string parentOAMModuleIPAddr);
bool setParentfstab(string UserStorageLoc, string UserStorageType, Devices devices, string DBRootStorageType, string parentOAMModuleName, string parentOAMModuleIPAddr, int serverTypeInstall, string sharedNothing, ChildModuleList childmodulelist);
bool setUMfstab(string parentOAMIPAddress, string moduleName, string TMPmount, string  exemgrMountLocation);
bool setPMfstab(string parentOAMIPAddress, Devices devices, string mountType, string moduleName);
bool updateBash();
bool makeModuleFile(string moduleName, string parentOAMModuleName);
bool updateProcessConfig(int serverTypeInstall);
bool uncommentCalpontXml( string entry);
bool makeRClocal(string moduleType, string moduleName, int IserverTypeInstall);
bool writeConfig(Config* sysConfig);
bool setPMexports(string moduleName, string parentOAMModuleIPAddr );
bool createDbrootDirs(string DBRootStorageType);
bool pkgCheck();
bool storageSetup();
void setSystemName();

string calpontPackage;
string mysqlPackage;
string mysqldPackage;

char* pcommand = 0;

string parentOAMModuleName;
int pmNumber = 0;

string sharedNothing;
string DBRootStorageLoc;
string DBRootStorageType;

string UserStorageType;
string UserStorageLoc;

int DBRootCount;
string deviceName;
Devices devices;

Config* sysConfig = Config::makeConfig();
string SystemSection = "SystemConfig";
string InstallSection = "Installation";
bool sharedNothingSupport = false;
string prompt;
string serverTypeInstall;
int    IserverTypeInstall;
string parentOAMModuleIPAddr;

string singleServerInstall = "1";
string reuseConfig ="n";
string oldFileName = "/usr/local/Calpont/etc/Calpont.xml.rpmsave";

extern string pwprompt;
bool noPrompting;

char* callReadline(string prompt)
{
	if ( !noPrompting )
		return readline(prompt.c_str());
	else {
		cout << prompt << endl;
		return "";
	}
}

void callFree(char* pcommand)
{
	if ( !noPrompting )
		free(pcommand);
}

ofstream fileOutputStream;		// this must be kept somewhere global

void redirectStandardOutputToFile ( string filePath, bool toPromptAlso )
{
	streambuf* sbuf = cout.rdbuf();
	fileOutputStream.open( filePath.c_str() );
	cout.rdbuf(fileOutputStream.rdbuf());
	if ( toPromptAlso )
		cout.rdbuf(sbuf);
}

int main(int argc, char *argv[])
{
    Oam oam;
	string parentOAMModuleHostName;
	ChildModuleList childmodulelist;
	ChildModule childmodule;
	PerformanceModuleList performancemodulelist;
	int DBRMworkernodeID = 0;
	string remote_installer_debug = "0";
	string EEPackageType = "";
	string nodeps = "-h";
	bool startOfflinePrompt = false;
	noPrompting = false;
	string password = "";

  	struct sysinfo myinfo; 

	// hidden options
	// -s for Shared-Nothing-Support
	// -f for force use nodeps on rpm install
	// -o to prompt for process to start offline

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
		cout << endl << "Your License trial period has expired, please contact Calpont Customer Support." << endl;
		exit (1);
	}

   for( int i = 1; i < argc; i++ )
   {
		if( string("-h") == argv[i] ) {
			cout << endl;
			cout << "This is the Calpont InfiniDB System Configuration and Installation tool." << endl;
			cout << "It will Configure the Calpont InfiniDB System based on Operator inputs and" << endl;
			cout << "will perform a Package Installation of all of the Modules within the" << endl;
			cout << "System that is being configured." << endl;
			cout << endl;
			cout << "IMPORTANT: This tool should only be run on a Performance Module Server," << endl;
			cout << "           preferably Module #1" << endl;
			cout << endl;
			cout << "Instructions:" << endl << endl;
			cout << "	Press 'enter' to accept a value in (), if available or" << endl;
			cout << "	Enter one of the options within [], if available, or" << endl;
			cout << "	Enter a new value" << endl << endl;
			cout << endl;
   			cout << "Usage: postConfigure [-h][-c][-n][-p][-d]" << endl;
			cout << "   -h  help" << endl;
			cout << "   -c  Config File to use to extract configuration data, default is Calpont.xml.rpmsave" << endl;
			cout << "   -n  Install with no prompting" << endl;
			cout << "	-p  Password can be entered as a command line option, optionally used with the no-prompting option"  << endl;
			cout << "   -d  Debug (outputs more information during remote module install section)" << endl;
			exit (0);
		}
      	else
			if( string("-d") == argv[i] )
				remote_installer_debug = "1";
			else if( string("-f") == argv[i] )
				nodeps = "--nodeps";
			else if( string("-s") == argv[i] )
				sharedNothingSupport = true;
			else if( string("-o") == argv[i] )
				startOfflinePrompt = true;
			else if( string("-c") == argv[i] ) {
				i++;
				oldFileName = argv[i];
				if ( oldFileName.find("Calpont.xml") == 0 ) {
					cout << "   ERROR: Config File is not a Calpont.xml file name" << endl;
					exit (1);
				}			
			}
			else if( string("-p") == argv[i] ) {
				i++;
				password = argv[i];
			}
			else
				if( string("-n") == argv[i] )
					noPrompting = true;
	}

	cout << endl;
	cout << "This is the Calpont InfiniDB System Configuration and Installation tool." << endl;
	cout << "It will Configure the Calpont InfiniDB System and will perform a Package" << endl;
	cout << "Installation of all of the Servers within the System that is being configured." << endl;
	cout << endl;

	cout << "IMPORTANT: This tool should only be run on the Parent OAM Module" << endl;
	cout << "           which is a Performance Module, preferred Module #1" << endl;
	cout << endl;

	if (!noPrompting) {
		cout << "Prompting instructions:" << endl << endl;
		cout << "	Press 'enter' to accept a value in (), if available or" << endl;
		cout << "	Enter one of the options within [], if available, or" << endl;
		cout << "	Enter a new value" << endl << endl;
	}
	else
	{
		//get current time and date
		time_t now;
		now = time(NULL);
		struct tm tm;
		localtime_r(&now, &tm);
		char timestamp[200];
		strftime (timestamp, 200, "%m:%d:%y-%H:%M:%S", &tm);
		string currentDate = timestamp;

		string postConfigureLog = "/var/log/infinidb-postconfigure-" + currentDate;

		cout << "With the no-Prompting Option being specified, you will be required to have the following:" << endl;
		cout << endl;
		cout << " 1. Passing the root password as a command line option or setting up a root user ssh" << endl;
		cout <<      "key between all nodes in the system." << endl;
		cout << " 2. A Configure File to use to retrieve configure data, default to Calpont.xml.rpmsave" << endl;
		cout << "    or use the '-c' option to point to a configuration file." << endl;
		cout << endl;
//		cout << " Output if being redirected to " << postConfigureLog << endl;

//		redirectStandardOutputToFile(postConfigureLog, false );

	}

	system("/usr/local/Calpont/bin/infinidb status > /tmp/status.log");
	if (oam.checkLogStatus("/tmp/status.log", "InfiniDB is running") ) {
		cout << "InfiniDB is running, can't run postConfigure while InfiniDB is running. Exiting.." << endl;
		exit (0);
	}

	//if InitialInstallFlag is set, then an install has been done
	// run pre-uninstall to clean from previous install and continue
	try {
		string InitialInstallFlag = sysConfig->getConfig(InstallSection, "InitialInstallFlag");
		if ( InitialInstallFlag == "y" ) {
			system("/usr/local/Calpont/bin/pre-uninstall skipxml > /dev/null 2>&1");
		}
	}
	catch(...)
	{}

	//run post install for coverage of possible binary package install
	system("/usr/local/Calpont/bin/post-install  > /dev/null 2>&1");

	//check Config saved files
	if ( !checkSaveConfigFile())
	{
		cout << "ERROR: Configuration File not setup" << endl;
		exit(1);
	}

	cout << endl;

	cout << "===== Setup System Server Type Configuration =====" << endl << endl;

	cout << "There are 2 options when configuring the System Server Type: single and multi" << endl << endl;
	cout << "  'single'  - Single-Server install is used when there will only be 1 server configured" << endl;
	cout << "              on the system. It's a shorter install procedure used for POC testing, as an example." << endl;
	cout << "              It can also be used for production systems, if the plan is to stay single-server." << endl << endl;
	cout << "  'multi'   - Multi-Server install is used when you want to configure multiple servers now or" << endl;
	cout << "              in the future. With Multi-Server install, you can still configure just 1 server" << endl;
	cout << "              now and add on addition servers/modules in the future. This is used more for" << endl;
	cout << "              production installs." << endl << endl;

	while(true) {
		prompt = "Select the type of System Server install [1=single, 2=multi] (" + singleServerInstall + ") > ";
		pcommand = callReadline(prompt);
		string temp;
		if (pcommand) {
			if (strlen(pcommand) > 0) 
				temp = pcommand;
			else
				temp = singleServerInstall;
			callFree(pcommand);
			pcommand = 0;
			if (temp == "1") {
				singleServerInstall = temp;
				cout << endl << "Performing the Single Server Install." << endl; 

				if ( reuseConfig == "n" ) {
					//setup to use the single server Calpont.xml file

					system("rm -f /usr/local/Calpont/etc/Calpont.xml.installSave  > /dev/null 2>&1");
					system("mv -f /usr/local/Calpont/etc/Calpont.xml /usr/local/Calpont/etc/Calpont.xml.installSave  > /dev/null 2>&1");
					system("/bin/cp -f /usr/local/Calpont/etc/Calpont.xml.singleserver  /usr/local/Calpont/etc/Calpont.xml  > /dev/null 2>&1");
				}

				setSystemName();
				cout << endl;

				//store syslog config file in Calpont Config file
				system("/usr/local/Calpont/bin/syslogSetup.sh install  > /dev/null 2>&1");

				// setup storage
				if (!storageSetup())
				{
					cout << "ERROR: Problem setting up storage" << endl;
					exit(1);
				}

				//check 'files per parition' with number of dbroots
				checkFilesPerPartion(DBRootCount, sysConfig);

				//check if dbrm data resides in older directory path and inform user if it does
				dbrmDirCheck();
				system("umount /usr/local/Calpont/data1  > /dev/null 2>&1");

				//check snmp Apps disable option
				snmpAppCheck();
			
				if (startOfflinePrompt)
					offLineAppCheck();

				if ( !writeConfig(sysConfig) ) {
					cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
					exit(1);
				}

				cout << endl << "===== Performing Configuration Setup and InfiniDB Startup =====" << endl;

				string cmd = "/usr/local/Calpont/bin/installer dummy.rpm dummy.rpm dummy.rpm initial dummy " + reuseConfig + " --nodeps ' ' 1";
				system (cmd.c_str());
				exit(0);
			}
			else
			{
				if (temp == "2") {
					singleServerInstall = temp;
					break;
				}
			}
			cout << "Invalid Entry, please re-enter" << endl;
			if ( noPrompting )
				exit(1);

			continue;
		}
		break;
	}

	try {
		sysConfig->setConfig(InstallSection, "SingleServerInstall", "n");
	}
	catch(...)
	{
		cout << "ERROR: Problem setting SingleServerInstall from the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}

	cout << endl << "===== Setup System Module Type Configuration =====" << endl << endl;

	cout << "There are 2 options when configuring the System Module Type: separate and combined" << endl << endl;
	cout << "  'separate' - User and Performance functionality on separate servers." << endl << endl;
	cout << "  'combined' - User and Performance functionality on the same server" << endl << endl;

	try {
		serverTypeInstall = sysConfig->getConfig(InstallSection, "ServerTypeInstall");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting ServerTypeInstall from the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//need for backward compatibility through 0.9.3.0
	if ( serverTypeInstall == "3" ||
			serverTypeInstall == "4" )
		// change 3/4 to a 1
		serverTypeInstall = "1";

	while(true)
	{
		prompt =  "Select the type of System Module Install [1=separate, 2=combined] (" + serverTypeInstall + ") > ";
		pcommand = callReadline(prompt);
		cout << endl;
		if (pcommand)
		{
			if (strlen(pcommand) > 0) serverTypeInstall = pcommand;
			callFree(pcommand);
			pcommand = 0;
		}

		if ( serverTypeInstall != "1" && serverTypeInstall != "2" ) {
			cout << "Invalid Entry, please enter 1 - 2" << endl << endl;
			serverTypeInstall = "1";
			if ( noPrompting )
				exit(1);
			continue;
		}

		IserverTypeInstall = atoi(serverTypeInstall.c_str());

		// set Server Type Installation Indicator
		try {
			sysConfig->setConfig(InstallSection, "ServerTypeInstall", serverTypeInstall);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting ServerTypeInstall in the InfiniDB System Configuration file" << endl;
			exit(1);
		}

		switch ( IserverTypeInstall ) {
			case (oam::INSTALL_COMBINE_DM_UM_PM):	// combined #1 - dm/um/pm on a single server
			{
				cout << "Combined Server Installation will be performed." << endl;
				cout << "The Server will be configured as a Performance Module." << endl;
				cout << "All InfiniDB Processes will run on the Performance Modules." << endl;
	
				//module ProcessConfig.xml to setup all apps on the dm
				if( !updateProcessConfig(IserverTypeInstall) )
					cout << "Update ProcessConfig.xml error" << endl;
	
				// are we using settings from previous config file?
				if ( reuseConfig == "n" ) {
					if( !uncommentCalpontXml("NumBlocksPct") ) {
						cout << "Update Calpont.xml NumBlocksPct Section" << endl;
						exit(1);
					}
	
					try {
						sysConfig->setConfig("DBBC", "NumBlocksPct", "50");

						cout << endl << "NOTE: Setting 'NumBlocksPct' to 50%" << endl;
					}
					catch(...)
					{
						cout << "ERROR: Problem setting NumBlocksPct in the InfiniDB System Configuration file" << endl;
						exit(1);
					}

					try{
						sysinfo(&myinfo);
					}
					catch (...) {}
			
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
				
					cout << "      Setting 'TotalUmMemory' to 25% of total memory (Combined Server Install maximum value is 16G) . Value set to " << value << endl;
	
					try {
						sysConfig->setConfig("HashJoin", "TotalUmMemory", value);
					}
					catch(...)
					{
						cout << "ERROR: Problem setting TotalUmMemory in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
	
					if ( !writeConfig(sysConfig) ) {
						cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
						exit(1);
					}
				}
				else
				{
					try {
						string numBlocksPct = sysConfig->getConfig("DBBC", "NumBlocksPct");
						string totalUmMemory = sysConfig->getConfig("HashJoin", "TotalUmMemory");

						if ( numBlocksPct.empty() )
							numBlocksPct = "86";

						cout << endl << "NOTE: Using previous configuration setting for 'NumBlocksPct' = " << numBlocksPct << "%" << endl;
						cout << "      Using previous configuration setting for 'TotalUmMemory' = " << totalUmMemory << endl;
						
					}
					catch(...)
					{
						cout << "ERROR: Problem reading NumBlocksPct/TotalUmMemory in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
				}
				break;
			}
			default:	// normal, separate UM and PM
			{
				// are we using settings from previous config file?
				if ( reuseConfig == "n" ) {
					cout << "NOTE: Using the default setting for 'NumBlocksPct' at 86%" << endl;
	
					try{
						sysinfo(&myinfo);
					}
					catch (...) {}
			
					//get memory stats
					long long total = myinfo.totalram / 1024 / 1000;
				
					// adjust max memory, 50% of total memory
					string value;
				
					if ( total <= 2000 )
						value = "512M";
					else
						if ( total <= 4000 )
							value = "1G";
						else
							if ( total <= 8000 )
								value = "2G";
							else
								if ( total <= 16000 )
									value = "4G";
								else
									if ( total <= 32000 )
										value = "8G";
									else
										if ( total <= 64000 )
											value = "16G";
										else
											value = "32G";
				
					cout << "      Setting 'TotalUmMemory' to 50% of total memory (separate Server Install maximum value is 32G). Value set to " << value << endl;
	
					try {
						sysConfig->setConfig("HashJoin", "TotalUmMemory", value);
					}
					catch(...)
					{
						cout << "ERROR: Problem setting TotalUmMemory in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
	
					if ( !writeConfig(sysConfig) ) {
						cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
						exit(1);
					}
				}
				else
				{
					try {
						string numBlocksPct = sysConfig->getConfig("DBBC", "NumBlocksPct");
						string totalUmMemory = sysConfig->getConfig("HashJoin", "TotalUmMemory");

						if ( numBlocksPct.empty() )
							numBlocksPct = "86";

						cout << endl << "NOTE: Using previous configuration setting for 'NumBlocksPct' = " << numBlocksPct << "%" << endl;
						cout << "      Using previous configuration setting for 'TotalUmMemory' = " << totalUmMemory << endl;
						
					}
					catch(...)
					{
						cout << "ERROR: Problem reading NumBlocksPct/TotalUmMemory in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
				}	
				break;
			}
		}
		break;
	}

	//store syslog config file in Calpont Config file
	system("/usr/local/Calpont/bin/syslogSetup.sh install  > /dev/null 2>&1");

	//Write out Updated System Configuration File
	try {
		sysConfig->setConfig(InstallSection, "InitialInstallFlag", "n");
	}
	catch(...)
	{
		cout << "ERROR: Problem setting InitialInstallFlag from the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	if ( !writeConfig(sysConfig) ) { 
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl; 
		exit(1);
	}
	
	setSystemName();

	cout << endl;

	//get Parent OAM Module Name
	string parentOAMModuleName;
	try{
		parentOAMModuleName = sysConfig->getConfig(SystemSection, "ParentOAMModuleName");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting ParentOAMModuleName the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	while(true) {
		string newparentOAMModuleName = parentOAMModuleName;
		prompt = "Enter the Local Module Name or exit [pmx,exit] (" + parentOAMModuleName +") > ";
		pcommand = callReadline(prompt);
		if (pcommand) {
			if (strlen(pcommand) > 0) newparentOAMModuleName = pcommand;
			callFree(pcommand);
			pcommand = 0;
			if (newparentOAMModuleName == "exit")
				exit(0);
		}
		else
			exit(0);
		if ( newparentOAMModuleName.find("pm") == 0 ) {
			parentOAMModuleName = newparentOAMModuleName;
			break;
		}
		cout << "Invalid Module Name, please re-enter" << endl;
		if ( noPrompting )
			exit(1);
	}

	try{
		 sysConfig->setConfig(SystemSection, "ParentOAMModuleName", parentOAMModuleName);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting ParentOAMModuleName the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	// set Standby Parent OAM module and IP to unassigned
 	try{
		 sysConfig->setConfig(SystemSection, "StandbyOAMModuleName", oam::UnassignedName);
		 sysConfig->setConfig("ProcStatusControlStandby", "IPAddr", oam::UnassignedIpAddr);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting ParentStandbyOAMModuleName the InfiniDB System Configuration file" << endl;
		exit(1);
	}


	//cleanup/create /usr/local/Calpont/local/etc  directory
	system("rm -rf /usr/local/Calpont/local/etc > /dev/null 2>&1");
	system("mkdir /usr/local/Calpont/local/etc > /dev/null 2>&1");

	//create associated /usr/local/Calpont/local/etc directory for parentOAMModuleName
	string cmd = "mkdir /usr/local/Calpont/local/etc/" + parentOAMModuleName + " > /dev/null 2>&1";
	system(cmd.c_str());

	//setup local module file name
	if( !makeModuleFile(parentOAMModuleName, parentOAMModuleName) ) {
		cout << "makeModuleFile error" << endl;
		exit(1);
	}

	cout << endl;

	if (startOfflinePrompt)
		offLineAppCheck();

	string parentOAMModuleType = parentOAMModuleName.substr(0,MAX_MODULE_TYPE_SIZE);
	int parentOAMModuleID = atoi(parentOAMModuleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
	if ( parentOAMModuleID < 1 ) {
		cout << "ERROR: Invalid Module ID of less than 1" << endl;
		exit(1);
	}

	//Get list of configured system modules
	SystemModuleTypeConfig sysModuleTypeConfig;

	try{
		oam.getSystemConfig(sysModuleTypeConfig);
	}
	catch(...)
	{
		cout << "ERROR: Problem reading the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	// 
	// setup storage
	//

	if (!storageSetup())
	{
		cout << "ERROR: Problem setting up storage" << endl;
		exit(1);
	}

	//check 'files per parition' with number of dbroots
	checkFilesPerPartion(DBRootCount, sysConfig);

	//Write out Updated System Configuration File
	try {
		sysConfig->setConfig(InstallSection, "InitialInstallFlag", "y");
	}
	catch(...)
	{
		cout << "ERROR: Problem setting InitialInstallFlag from the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//
	// Module Configuration
	//
	cout << endl;
	cout << "===== Setup the Module Configuration =====" << endl;

	string ModuleSection = "SystemModuleConfig";

	//get OAM Parent Module IP addresses and Host Name, if it exist
	for ( unsigned int i = 0 ; i < sysModuleTypeConfig.moduletypeconfig.size(); i++)
	{
		DeviceNetworkList::iterator listPT = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
		for( ; listPT != sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; listPT++)
		{
			HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
	
			if ( (*listPT).DeviceName == parentOAMModuleName ) {
				parentOAMModuleIPAddr = (*pt1).IPAddr;
				parentOAMModuleHostName = (*pt1).HostName;
				break;
			}
		}
	}

	unsigned int maxPMNicCount = 1;

	//configure module type
	bool parentOAMmoduleConfig = false;
	bool moduleSkip = false;

	for ( unsigned int i = 0 ; i < sysModuleTypeConfig.moduletypeconfig.size(); i++)
	{
		string moduleType = sysModuleTypeConfig.moduletypeconfig[i].ModuleType;
		string moduleDesc = sysModuleTypeConfig.moduletypeconfig[i].ModuleDesc;
		int moduleCount = sysModuleTypeConfig.moduletypeconfig[i].ModuleCount;

		if ( moduleType == "dm") {
			moduleCount = 0;
			try {
				string ModuleCountParm = "ModuleCount" + oam.itoa(i+1);
				sysConfig->setConfig(ModuleSection, ModuleCountParm, oam.itoa(moduleCount));
				continue;
			}
			catch(...)
			{}
		}

		//verify and setup of modules count
		switch ( IserverTypeInstall ) {
			case (oam::INSTALL_COMBINE_DM_UM_PM):
			{
				if ( moduleType == "um") {
					moduleCount = 0;
					try {
						string ModuleCountParm = "ModuleCount" + oam.itoa(i+1);
						sysConfig->setConfig(ModuleSection, ModuleCountParm, oam.itoa(moduleCount));
						continue;
					}
					catch(...)
					{
						cout << "ERROR: Problem setting Module Count in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
				}
				break;
			}
			default:
				break;
		}

		cout << endl << "----- " << moduleDesc << " Configuration -----" << endl << endl;

		int oldModuleCount = moduleCount;
		while(true)
		{
			prompt = "Enter number of " + moduleDesc + "s (" + oam.itoa(oldModuleCount) + ") > ";
			moduleCount = oldModuleCount;
			pcommand = callReadline(prompt);
			if (pcommand) {
				if (strlen(pcommand) > 0) moduleCount = atoi(pcommand);
				callFree(pcommand);
				pcommand = 0;
			}

			if ( moduleCount < 0 ) {
				cout << endl << "ERROR: Invalid Module Count '" + oam.itoa(moduleCount) + "', please reenter" << endl << endl;
				if ( noPrompting )
					exit(1);
				continue;
			}

			if ( parentOAMModuleType == moduleType && moduleCount == 0 ) {
				cout << endl << "ERROR: Parent OAM Module Type is '" + parentOAMModuleType + "', so you have to have at least 1 of this Module Type, please reenter" << endl << endl;
				if ( noPrompting )
					exit(1);
				continue;
			}

			if ( moduleCount == 0 ) {
				string count0 = "n";
				while (true)
				{
					cout << endl;
					prompt = "System will not be functional with '0' number of " + moduleDesc + "s, do you want to re-enter a different Module Count? [y,n] (n) > ";
					pcommand = callReadline(prompt);
					if (pcommand)
					{
						if (strlen(pcommand) > 0) count0 = pcommand;
						callFree(pcommand);
						pcommand = 0;
					}
					if ( count0 == "y" || count0 == "n" ) {
						cout << endl;
						break;
					}
					else
						cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
					count0 = "n";
					if ( noPrompting )
						exit(1);
				}
				if ( count0 == "y" )
					continue;
			}

			//update count
			try {
				string ModuleCountParm = "ModuleCount" + oam.itoa(i+1);
				sysConfig->setConfig(ModuleSection, ModuleCountParm, oam.itoa(moduleCount));
				break;
			}
			catch(...)
			{
				cout << "ERROR: Problem setting Module Count in the InfiniDB System Configuration file" << endl;
				exit(1);
			}
		}

		int listSize = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.size();

		if ( moduleCount == 0 ) {
			//clear Equipped Module IP addresses
			for ( int j = 0 ; j < listSize ; j++ )
			{
				for ( unsigned int k = 1 ; k < MAX_NIC+1 ; k++ )
				{
					string ModuleIPAddr = "ModuleIPAddr" + oam.itoa(j+1) + "-" + oam.itoa(k) + "-" + oam.itoa(i+1);
					if ( !(sysConfig->getConfig(ModuleSection, ModuleIPAddr).empty()) ) {
						string ModuleHostName = "ModuleHostName" + oam.itoa(j+1) + "-" + oam.itoa(k) + "-" + oam.itoa(i+1);

						sysConfig->setConfig(ModuleSection, ModuleIPAddr, oam::UnassignedIpAddr);
						sysConfig->setConfig(ModuleSection, ModuleHostName, oam::UnassignedName);
					}
				}
			}

			//no modules equipped for this Module Type, skip
			continue;
		}

		if ( moduleType == "pm" )
			pmNumber = moduleCount;

		//Enter User Temp Storage location
		if ( moduleType == "um" || 
				( moduleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM ) ||
				( moduleType == "um" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM ) ||
				( moduleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_PM_UM ) ) {

			try {
				UserStorageType = sysConfig->getConfig(InstallSection, "UserStorageType");
			}
			catch(...)
			{
				cout << "ERROR: Problem getting DB Storage Data from the InfiniDB System Configuration file" << endl;
				exit(1);
			}

			if ( moduleType == "um" )
				cout << endl;
/*
			while(true)
			{
				string newUserStorageType = UserStorageType;
				prompt = "Enter User Temp Data Storage Mount Type [storage,local] (" + UserStorageType + ") > ";
				pcommand = callReadline(prompt);
				if (pcommand)
				{
					if (strlen(pcommand) > 0) newUserStorageType = pcommand;
					callFree(pcommand);
					pcommand = 0;
				}

				if ( newUserStorageType == "storage" || newUserStorageType == "local" ) {
					UserStorageType = newUserStorageType;
					break;
				}
				cout << "Invalid Mount Type, please re-enter" << endl;
			}
		
			try {
				sysConfig->setConfig(InstallSection, "UserStorageType", UserStorageType);
			}
			catch(...)
			{
				cout << "ERROR: Problem setting UserStorageType in the InfiniDB System Configuration file" << endl;
				exit(1);
			}
*/		}

		int moduleID = 1;
		while(true) {
			prompt = "Enter Starting Module ID for " + moduleDesc + " (1) > ";
			pcommand = callReadline(prompt);
			if (pcommand)
			{
				if (strlen(pcommand) > 0) moduleID = atoi(pcommand);
				callFree(pcommand);
				pcommand = 0;
			}
	
			//valid if parent OAM module type and is consistent with parentOAMModuleName
			if ( parentOAMModuleType == moduleType && 
					( parentOAMModuleID < moduleID || parentOAMModuleID > moduleID + (moduleCount-1) ) ) {
				cout << endl << "ERROR: Parent and Starting Module ID out of range, please re-enter" << endl << endl;
				moduleID = 1;
				if ( noPrompting )
					exit(1);
			}
			else
				break;
		}

		//clear any Equipped Module IP addresses that aren't in current ID range
		for ( int j = 0 ; j < listSize ; j++ )
		{
			for ( unsigned int k = 1 ; k < MAX_NIC+1 ; k++)
			{
				string ModuleIPAddr = "ModuleIPAddr" + oam.itoa(j+1) + "-" + oam.itoa(k) + "-" + oam.itoa(i+1);
				if ( !(sysConfig->getConfig(ModuleSection, ModuleIPAddr).empty()) ) {
					if ( j+1 < moduleID || j+1 > moduleID + (moduleCount-1) ) {
						string ModuleHostName = "ModuleHostName" + oam.itoa(j+1) + "-" + oam.itoa(k) + "-" + oam.itoa(i+1);
	
						sysConfig->setConfig(ModuleSection, ModuleIPAddr, oam::UnassignedIpAddr);
						sysConfig->setConfig(ModuleSection, ModuleHostName, oam::UnassignedName);
					}
				}
			}
		}

		//configure module name
		while(true)
		{
			int saveModuleID = moduleID;
			for( int k=0 ; k < moduleCount ; k++, moduleID++)
			{
				string newModuleName = moduleType + oam.itoa(moduleID);

				if (moduleType == parentOAMModuleType && moduleID != parentOAMModuleID && !parentOAMmoduleConfig) {
					//skip this module for now, need to configure parent OAM Module first
					moduleSkip = true;
					continue;
				}

				if (moduleType == parentOAMModuleType && moduleID == parentOAMModuleID && parentOAMmoduleConfig)
					//skip, aleady configured
					continue;

				if (moduleType == parentOAMModuleType && moduleID == parentOAMModuleID && !parentOAMmoduleConfig)
					parentOAMmoduleConfig = true;

				string moduleNameDesc = moduleDesc + " #" + oam.itoa(moduleID);
				PerformanceModule performancemodule;
	
				if ( newModuleName == parentOAMModuleName )
					cout << endl << "*** Parent OAM Module " << moduleNameDesc << " Configuration ***" << endl << endl;
				else
					cout << endl << "*** " << moduleNameDesc << " Configuration ***" << endl << endl;

				if ( moduleType == "um" ||
					( moduleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM ) ||
					( moduleType == "um" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM ) ||
					( moduleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_PM_UM ) ) {
	
					UserStorageLoc = oam::UnassignedName;
					string USERSTORAGELOC = "UserStorageLoc" + oam.itoa(moduleID);
					if ( UserStorageType == "storage") {
						try {
							UserStorageLoc = sysConfig->getConfig(InstallSection, USERSTORAGELOC);
						}
						catch(...)
						{
							cout << "ERROR: Problem getting DB Storage Data from the InfiniDB System Configuration file" << endl;
							exit(1);
						}
		
						prompt = "Enter Device Name for User Temp Storage (" + UserStorageLoc + ") > ";
						pcommand = callReadline(prompt);
						if (pcommand)
						{
							if (strlen(pcommand) > 0) UserStorageLoc = pcommand;
							callFree(pcommand);	
							pcommand = 0;
						}
		
						try {
							sysConfig->setConfig(InstallSection, USERSTORAGELOC, UserStorageLoc);
						}
						catch(...)
						{
							cout << "ERROR: Problem setting UserStorageLoc in the InfiniDB System Configuration file" << endl;
							exit(1);
						}
					}
					else
					{
						try {
							sysConfig->setConfig(InstallSection, USERSTORAGELOC, oam::UnassignedName);
						}
						catch(...)
						{
							cout << "ERROR: Problem setting UserStorageLoc in the InfiniDB System Configuration file" << endl;
							exit(1);
						}
					}
				}
	
				string moduleDisableState = oam::ENABLEDSTATE;
	
				//setup HostName/IPAddress for each NIC
				for( unsigned int nicID=1 ; nicID < MAX_NIC+1 ; nicID++ )
				{
					string moduleHostName = oam::UnassignedName;
					string moduleIPAddr = oam::UnassignedIpAddr;
		
					DeviceNetworkList::iterator listPT = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
					for( ; listPT != sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; listPT++)
					{
						if (newModuleName == (*listPT).DeviceName) {
							moduleDisableState = (*listPT).DisableState;
							if ( moduleDisableState.empty() ||
								moduleDisableState == oam::UnassignedName ||
								moduleDisableState == oam::AUTODISABLEDSTATE )
								moduleDisableState = oam::ENABLEDSTATE;
	
							HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
							for( ; pt1 != (*listPT).hostConfigList.end() ; pt1++)
							{
								if ((*pt1).NicID == nicID) {
									moduleHostName = (*pt1).HostName;
									moduleIPAddr = (*pt1).IPAddr;
									break;
								}
							}
						}
					}
	
					if ( nicID == 1 && moduleDisableState != oam::ENABLEDSTATE ) {
						string disabled = "y";
						while (true)
						{
							prompt = "Module '" + newModuleName + "' is Disabled, do you want to leave it Disabled? [y,n] (y) > ";
							pcommand = callReadline(prompt);
							if (pcommand)
							{
								if (strlen(pcommand) > 0) disabled = pcommand;
								callFree(pcommand);
								pcommand = 0;
							}
							if ( disabled == "y" || disabled == "n" ) {
								cout << endl;
								break;
							}
							else
								cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
							disabled = "y";
							if ( noPrompting )
								exit(1);
						}
						if ( disabled == "n" ) {
							moduleDisableState = oam::ENABLEDSTATE;
	
							//set Module Disable State
							string moduleDisableStateParm = "ModuleDisableState" + oam.itoa(moduleID) + "-" + oam.itoa(i+1);
							try {
								sysConfig->setConfig(ModuleSection, moduleDisableStateParm, moduleDisableState);
							}
							catch(...)
							{
								cout << "ERROR: Problem setting ModuleDisableState in the InfiniDB System Configuration file" << endl;
								exit(1);
							}
						}
					}
	
					bool moduleHostNameFound = true;
					if (moduleHostName.empty()) {
						moduleHostNameFound = true;
						moduleHostName = oam::UnassignedName;
					}
	
					if (moduleIPAddr.empty())
						moduleIPAddr = oam::UnassignedIpAddr;
		
					string newModuleHostName;
					string newModuleIPAddr;
	
					while (true)
					{
						newModuleHostName = moduleHostName;
						prompt = "Enter Nic Interface #" + oam.itoa(nicID) + " Host Name (" + moduleHostName + ") > ";
						pcommand = callReadline(prompt);
						if (pcommand)
						{
							if (strlen(pcommand) > 0) newModuleHostName = pcommand;	
							callFree(pcommand);
							pcommand = 0;
						}
		
						if ( newModuleHostName == oam::UnassignedName && nicID == 1 )
							cout << "Invalid Entry, please enter at least 1 Host Name for Module" << endl;
						else
							break;
						if ( noPrompting )
							exit(1);
					}
	
					//set New Module Host Name
					string moduleHostNameParm = "ModuleHostName" + oam.itoa(moduleID) + "-" + oam.itoa(nicID) + "-" + oam.itoa(i+1);
					try {
						sysConfig->setConfig(ModuleSection, moduleHostNameParm, newModuleHostName);
					}
					catch(...)
					{
						cout << "ERROR: Problem setting Host Name in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
	
					if ( newModuleHostName == oam::UnassignedName )
						newModuleIPAddr = oam::UnassignedIpAddr;
					else
					{
						if ( noPrompting ) {
							if ( moduleIPAddr != oam::UnassignedIpAddr || !moduleIPAddr.empty() )
								newModuleIPAddr = moduleIPAddr;
						}

						if ( newModuleIPAddr != moduleIPAddr )
						{
							//get Network IP Address
							string IPAddress = oam.getIPAddress( newModuleHostName);
							if ( !IPAddress.empty() )
								newModuleIPAddr = IPAddress;
							else
								newModuleIPAddr = moduleIPAddr;
	
							if ( newModuleIPAddr == "127.0.0.1") {
								if ( moduleIPAddr != oam::UnassignedIpAddr || !moduleIPAddr.empty() )
									newModuleIPAddr = moduleIPAddr;
								else
									newModuleIPAddr = "unassigned";
							}
						}

						//prompt for IP address
						while (true)
						{
							prompt = "Enter Nic Interface #" + oam.itoa(nicID) + " IP Address of " + newModuleHostName + " (" + newModuleIPAddr + ") > ";
							pcommand = callReadline(prompt);
							if (pcommand)
							{
								if (strlen(pcommand) > 0) newModuleIPAddr = pcommand;	
								callFree(pcommand);
								pcommand = 0;
							}

							if (newModuleIPAddr == "127.0.0.1" || newModuleIPAddr == "0.0.0.0") {
								cout << endl << newModuleIPAddr + " is an Invalid IP Address for a multi-server system, please reenter" << endl << endl;
								newModuleIPAddr = "unassigned";
								if ( noPrompting )
									exit(1);
								continue;
							}

							if (oam.isValidIP(newModuleIPAddr))
								break;
							else
								cout << endl << "Invalid IP Address format, xxx.xxx.xxx.xxx, please reenter" << endl << endl;
							if ( noPrompting )
								exit(1);

						}
					}
	
					//set Module IP address
					string moduleIPAddrNameParm = "ModuleIPAddr" + oam.itoa(moduleID) + "-" + oam.itoa(nicID) + "-" + oam.itoa(i+1);
					try {
						sysConfig->setConfig(ModuleSection, moduleIPAddrNameParm, newModuleIPAddr);
					}
					catch(...)
					{
						cout << "ERROR: Problem setting IP address in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
	
					if ( newModuleHostName == oam::UnassignedName && moduleHostNameFound )
						// exit out to next module ID
						break;
	
					if (moduleType == "pm" && moduleDisableState == oam::ENABLEDSTATE) {
		
						switch(nicID) {
							case 1:
								performancemodule.moduleIP1 = newModuleIPAddr;
								break;
							case 2:
								performancemodule.moduleIP2 = newModuleIPAddr;
								break;
							case 3:
								performancemodule.moduleIP3 = newModuleIPAddr;
								break;
							case 4:
								performancemodule.moduleIP4 = newModuleIPAddr;
								break;
						}
						if ( maxPMNicCount < nicID )
							maxPMNicCount = nicID;
					}
	
					if ( nicID > 1 )
						continue;
		
					//save Child modules
					if ( newModuleName != parentOAMModuleName) {
						childmodule.moduleName = newModuleName;
						childmodule.moduleIP = newModuleIPAddr;
						childmodule.hostName = newModuleHostName;
						childmodulelist.push_back(childmodule);
					}
	
					//set port addresses
					if ( newModuleName == parentOAMModuleName ) {
						if ( parentOAMModuleHostName == oam::UnassignedName ||
								parentOAMModuleHostName.empty() ) {
							parentOAMModuleHostName = newModuleHostName;
							parentOAMModuleIPAddr = newModuleIPAddr;
						}

						//set Parent Processes Port IP Address
						string parentProcessMonitor = parentOAMModuleName + "_ProcessMonitor";
						sysConfig->setConfig(parentProcessMonitor, "IPAddr", parentOAMModuleIPAddr);
						sysConfig->setConfig(parentProcessMonitor, "Port", "8800");
						sysConfig->setConfig("ProcMgr", "IPAddr", parentOAMModuleIPAddr);
						//sysConfig->setConfig("ProcHeartbeatControl", "IPAddr", parentOAMModuleIPAddr);
						sysConfig->setConfig("ProcStatusControl", "IPAddr", parentOAMModuleIPAddr);
						string parentServerMonitor = parentOAMModuleName + "_ServerMonitor";
						sysConfig->setConfig(parentServerMonitor, "IPAddr", parentOAMModuleIPAddr);
	
						if( IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM ) {
	
							//set User Module's IP Addresses
							string Section = "ExeMgr" + oam.itoa(moduleID);
			
							sysConfig->setConfig(Section, "IPAddr", newModuleIPAddr);
							sysConfig->setConfig(Section, "Port", "8601");
			
							//set Performance Module's IP's to first NIC IP entered
							sysConfig->setConfig("DDLProc", "IPAddr", newModuleIPAddr);
							sysConfig->setConfig("DMLProc", "IPAddr", newModuleIPAddr);
						}

						//setup rc.local file in module tmp dir
						if( !makeRClocal(moduleType , newModuleName, IserverTypeInstall) )
							cout << "makeRClocal error" << endl;
					}
					else
					{
						//set child Process Monitor Port IP Address
						string portName = newModuleName + "_ProcessMonitor";
						sysConfig->setConfig(portName, "IPAddr", newModuleIPAddr);
	
						sysConfig->setConfig(portName, "Port", "8800");
	
						//set child Server Monitor Port IP Address
						portName = newModuleName + "_ServerMonitor";
						sysConfig->setConfig(portName, "IPAddr", newModuleIPAddr);
						sysConfig->setConfig(portName, "Port", "8622");
		
						//set User Module's IP Addresses
						if ( moduleType == "um" ||
							( moduleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM ) ||
							( moduleType == "um" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM ) ||
							( moduleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_PM_UM ) ) {
	
							string Section = "ExeMgr" + oam.itoa(moduleID);
		
							sysConfig->setConfig(Section, "IPAddr", newModuleIPAddr);
							sysConfig->setConfig(Section, "Port", "8601");
						}
		
						//set Performance Module's IP's to first NIC IP entered
						if ( newModuleName == "pm1" ) {
							sysConfig->setConfig("DDLProc", "IPAddr", newModuleIPAddr);
							sysConfig->setConfig("DMLProc", "IPAddr", newModuleIPAddr);
						}
					}
					
					if ( !writeConfig(sysConfig) ) {
						cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
						exit(1);
					}

					//create associated /usr/local/Calpont/local/etc directory for module
					string cmd = "mkdir /usr/local/Calpont/local/etc/" + newModuleName + " > /dev/null 2>&1";
					system(cmd.c_str());
			
					//setup remote module mount files
					if ( moduleType == "um" ) {						
						//setup UM fstab in /usr/local/Calpont/local/etc directories
						if( !setUMfstab(parentOAMModuleIPAddr, newModuleName, UserStorageLoc, UserStorageType ) )
							cout << "setUMfstab error" << endl;
					}
					else
					{
						if ( moduleType == "pm" && newModuleName != parentOAMModuleName ) {
							//setup PM fstab in /usr/local/Calpont/local/etc directories
							if( !setPMfstab(parentOAMModuleIPAddr, devices, DBRootStorageType, newModuleName) )
								cout << "setPMfstab error" << endl;
						}
					}

					if ( newModuleName != parentOAMModuleName) {		
						//make module file in /usr/local/Calpont/local/etc/"modulename"
						if( !makeModuleFile(newModuleName, parentOAMModuleName) )
							cout << "makeModuleFile error" << endl;
					}
	
					//setup rc.local file in module tmp dir
					if( !makeRClocal(moduleType , newModuleName, IserverTypeInstall) )
						cout << "makeRClocal error" << endl;
	
					//setup DBRM Processes
					if ( newModuleName == parentOAMModuleName )
						sysConfig->setConfig("DBRM_Controller", "IPAddr", newModuleIPAddr);
	
					if ( moduleDisableState == oam::ENABLEDSTATE ) {
						DBRMworkernodeID++;
						string DBRMSection = "DBRM_Worker" + oam.itoa(DBRMworkernodeID);
						sysConfig->setConfig(DBRMSection, "IPAddr", newModuleIPAddr);
						sysConfig->setConfig(DBRMSection, "Module", newModuleName);
					}	
				} //end of nicID loop
	
				if (moduleType == "pm" && moduleDisableState == oam::ENABLEDSTATE )
					performancemodulelist.push_back(performancemodule);
			} //end of k (moduleCount) loop
			if ( moduleSkip ) {
				moduleSkip = false;
				moduleID = saveModuleID;
			}
			else
				break;
		}

		if ( !writeConfig(sysConfig) ) {
			cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
			exit(1);
		}
	

	} //end of i for loop

	//set child exports file
	ChildModuleList::iterator list1 = childmodulelist.begin();
	for (; list1 != childmodulelist.end() ; list1++)
	{
		if ( (*list1).moduleName.substr(0,MAX_MODULE_TYPE_SIZE) == "pm" &&
				sharedNothing == "y") {
			if( !setPMexports((*list1).moduleName, parentOAMModuleIPAddr ) )
				cout << "setPMexports error" << endl;
		}
	}

	// setup local PM fstab file
	if( !setParentfstab(UserStorageLoc, UserStorageType, devices, DBRootStorageType, parentOAMModuleName, parentOAMModuleIPAddr, IserverTypeInstall, sharedNothing, childmodulelist) )
		cout << "setParentfstab error" << endl;

	//setup DBRM Controller
	sysConfig->setConfig("DBRM_Controller", "NumWorkers", oam.itoa(DBRMworkernodeID));

	//set ConnectionsPerPrimProc
	try {
		sysConfig->setConfig("PrimitiveServers", "ConnectionsPerPrimProc", oam.itoa(maxPMNicCount*2));
	}
	catch(...)
	{
		cout << "ERROR: Problem setting ConnectionsPerPrimProc in the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	//setup DBRoot paths for shared-nothing configuration
	if ( sharedNothing == "y" ) {
		DBRootCount = pmNumber;
		try {
			sysConfig->setConfig(SystemSection, "DBRootCount", oam.itoa(pmNumber));
		}
		catch(...)
		{
			cout << "ERROR: Problem setting DBRoot Count in the InfiniDB System Configuration file" << endl;
			exit(1);
		}

		for ( int i = 1 ; i < pmNumber+1 ; i++ )
		{
			string DBrootID = "DBRoot" + oam.itoa(i);
			string pathID =  "/usr/local/Calpont/data" + oam.itoa(i);
	
			try {
				sysConfig->setConfig(SystemSection, DBrootID, pathID);
			}
			catch(...)
			{
				cout << "ERROR: Problem setting DBRoot in the InfiniDB System Configuration file" << endl;
				exit(1);
			}
			string cmd = "mkdir -p " + pathID;
			system(cmd.c_str());

		}

		//clear out any additional dbroots entries
		for ( int i = pmNumber+1 ; i < 100 ; i++ )
		{
			string DBrootID = "DBRoot" + oam.itoa(i);
	
			try {
				string entry = sysConfig->getConfig(SystemSection, DBrootID);
				if ( !entry.empty())
					sysConfig->setConfig(SystemSection, DBrootID, "unavailable");
				else
					break;
			}
			catch(...)
			{
				break;
			}
		}
	}

	//set the PM Ports based on Number of PM modules equipped, if any equipped
	int pmPorts = 32;
	sysConfig->setConfig("PrimitiveServers", "Count", oam.itoa(pmNumber));

	if ( pmNumber > 0 )
	{
		const string PM = "PMS";

		for ( int pmsID = 1; pmsID < pmPorts+1 ; )
		{
			for (unsigned int j = 1 ; j < maxPMNicCount+1 ; j++)
			{
				PerformanceModuleList::iterator list1 = performancemodulelist.begin();
		
				for (; list1 != performancemodulelist.end() ; list1++)
				{
					string pmName = PM + oam.itoa(pmsID);
					string IpAddr;
					switch(j) {
						case 1:
							IpAddr = (*list1).moduleIP1;
							break;
						case 2:
							IpAddr = (*list1).moduleIP2;
							break;
						case 3:
							IpAddr = (*list1).moduleIP3;
							break;
						case 4:
							IpAddr = (*list1).moduleIP4;
							break;
					}
					if ( !IpAddr.empty() && IpAddr != oam::UnassignedIpAddr ) {
						sysConfig->setConfig(pmName, "IPAddr", IpAddr);
						pmsID++;
						if ( pmsID > pmPorts )
							break;
					}
				}
				if ( pmsID > pmPorts )
					break;
			}
		}
	}

	//check snmp Apps disable option
	snmpAppCheck();

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}
	

	//
	// Configure External Devices
	//
	SystemExtDeviceConfig systemextdeviceconfig;

	try{
		oam.getSystemConfig(systemextdeviceconfig);
	}
	catch(...)
	{
		cout << "ERROR: Problem reading the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	cout << endl << "===== Setup the External Device Configuration =====" << endl << endl;

	cout << "External Devices are devices like a storage array or a Ethernet Switch that can" << endl;
	cout << "be setup to be monitored by InfiniDB with a ping test. If device fails, InfiniDB" << endl;
	cout << "will report a failure alarm." << endl << endl;

	if ( systemextdeviceconfig.Count > 0 ) {

		cout << "Current List of External Devices being monitored" << endl << endl;
	
		cout << "Device Name    IP Address" << endl;
		cout << "--------------------------    ----------------" << endl;

		for ( unsigned int i = 0 ; i < systemextdeviceconfig.Count ; i++ )
		{
			cout.setf(ios::left);
			cout.width(30);
			cout << systemextdeviceconfig.extdeviceconfig[i].Name;
			cout << systemextdeviceconfig.extdeviceconfig[i].IPAddr << endl;
//			cout << "Device Name: " << (*pt1).Name << "     IP Address: " << (*pt1).IPAddr << endl;
		}
	
		cout << endl;
	
		string modify = "n";
		while(true) {
			prompt = "Would you like to modify current list? [y,n] (n) > ";
			pcommand = callReadline(prompt);
			if (pcommand)
			{
				if (strlen(pcommand) > 0) modify = pcommand;
				callFree(pcommand);
				pcommand = 0;
			}
			if ( modify == "y" || modify == "n" )
				break;
			cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
			if ( noPrompting )
				exit(1);
		}
	
		if ( modify == "y" ) {
	
			for ( unsigned int i = 0 ; i < systemextdeviceconfig.Count ; i++ )
			{
				string name = systemextdeviceconfig.extdeviceconfig[i].Name;
				modify = "n";
				while(true) {
					prompt = "Would you like to modify or delete '" + name + "'? [m,d,n] (n) > ";
					pcommand = callReadline(prompt);
					if (pcommand)
					{
						if (strlen(pcommand) > 0) modify = pcommand;
						callFree(pcommand);
						pcommand = 0;
					}
					if ( modify == "m" || modify == "d" || modify == "n")
						break;
					cout << "Invalid Entry, please enter 'm' for modify, 'd' for delete or 'n' for no" << endl;
					if ( noPrompting )
						exit(1);
				}
				
				if ( modify == "d" ) {
					// delete device
					ExtDeviceConfig extdeviceconfig;
	
					extdeviceconfig.Name = oam::UnassignedName;
					extdeviceconfig.IPAddr = oam::UnassignedIpAddr;
					extdeviceconfig.DisableState = oam::ENABLEDSTATE;
	
					try{
						oam.setSystemConfig(name, extdeviceconfig);
					}
					catch(...)
					{
						cout << "ERROR: Problem updating the InfiniDB System Configuration file" << endl;
						exit(1);
					}
					cout << "External Device '" + name + "' deleted" << endl << endl;
				}
				else
				{
					if ( modify == "m" ) {
						string newName = name;
						prompt = "Enter Name (" + newName + ") > ";
						pcommand = callReadline(prompt);
						if (pcommand)
						{
							if (strlen(pcommand) > 0) newName = pcommand;
							callFree(pcommand);
							pcommand = 0;
						}
	
						string newIPAddr = systemextdeviceconfig.extdeviceconfig[i].IPAddr;
						while (true)
						{
							prompt = "Enter IP Address of (" + newIPAddr + ") > ";
							pcommand = callReadline(prompt);
							if (pcommand)
							{
								if (strlen(pcommand) > 0) newIPAddr = pcommand;
								callFree(pcommand);
								pcommand = 0;
							}
	
							if (oam.isValidIP(newIPAddr))
								break;
							else
								cout << "Invalid IP Address format, xxx.xxx.xxx.xxx, please reenter" << endl;
							if ( noPrompting )
								exit(1);
						}
	
						ExtDeviceConfig extdeviceconfig;
		
						extdeviceconfig.Name = newName;
						extdeviceconfig.IPAddr = newIPAddr;
						extdeviceconfig.DisableState = oam::ENABLEDSTATE;
		
						try{
							oam.setSystemConfig(name, extdeviceconfig);
						}
						catch(...)
						{
							cout << "ERROR: Problem updating the InfiniDB System Configuration file" << endl;
							exit(1);
						}
						cout << "External Device '" + name + "' modified" << endl << endl;
					}
				}
			}
		}
	}

	while(true) {
		string add = "n";
		while(true) {
			prompt = "Would you like to add an External Device? [y,n] (n) > ";
			pcommand = callReadline(prompt);
			if (pcommand)
			{
				if (strlen(pcommand) > 0) add = pcommand;
				callFree(pcommand);
				pcommand = 0;
			}
			if ( add == "y" || add == "n" )
				break;
			cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
			if ( noPrompting )
				exit(1);
		}
	
		if ( add == "y" ) {
			cout << endl;
			string newName = oam::UnassignedName;
			while(true) {
				prompt = "Enter Name (" + newName + ") > ";
				pcommand = callReadline(prompt);
				if (pcommand)
				{
					if (strlen(pcommand) > 0) newName = pcommand;
					callFree(pcommand);
					pcommand = 0;
				}
				
				if ( newName == oam::UnassignedName )
					cout << "Invalid Entry, please enter valid name or 'abort'" << endl;
				else
					break;
				if ( noPrompting )
					exit(1);
			}

			if ( newName == "abort" )
				continue;

			//get Network IP Address
			string newIPAddr = oam::UnassignedIpAddr;
			string IPAddress = oam.getIPAddress(newName);
			if ( !IPAddress.empty() )
				newIPAddr = IPAddress;

			while (true)
			{
				prompt = "Enter IP Address of (" + newIPAddr + ") > ";
				pcommand = callReadline(prompt);
				if (pcommand)
				{
					if (strlen(pcommand) > 0) newIPAddr = pcommand;
					callFree(pcommand);
					pcommand = 0;
				}
		
				if (oam.isValidIP(newIPAddr))
					break;
				else
					cout << "Invalid IP Address format, xxx.xxx.xxx.xxx, please reenter" << endl;
				newIPAddr = oam::UnassignedIpAddr;
				if ( noPrompting )
					exit(1);
			}
		
			ExtDeviceConfig extdeviceconfig;
		
			extdeviceconfig.Name = newName;
			extdeviceconfig.IPAddr = newIPAddr;
			extdeviceconfig.DisableState = oam::ENABLEDSTATE;
		
			try{
				oam.setSystemConfig(newName, extdeviceconfig);
			}
			catch(...)
			{
				cout << "ERROR: Problem updating the InfiniDB System Configuration file" << endl;
				exit(1);
			}
			cout << endl;
		}
		else
			break;
	}

	//
	// setup NFS Service
	//
	if ( sharedNothing == "y" ) {
		system("chkconfig nfs on > /dev/null 2>&1");
		system("chkconfig --level 2 --level 3 --level 4 --level 5 nfs > /dev/null 2>&1");
		system("chkconfig nfslock on > /dev/null 2>&1");
		system("chkconfig --level 2 --level 3 --level 4 --level 5 nfslock > /dev/null 2>&1");
	}

	//setup local OS Files
	if( !setOSFiles(parentOAMModuleName, IserverTypeInstall, sharedNothing, DBRootStorageType) ) {
		cout << "setOSFiles error" << endl;
		exit(1);
	}

	//create directories on dbroot1
	if( !createDbrootDirs(DBRootStorageType) ) {
		cout << "createDbrootDirs error" << endl;
		exit(1);
	}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}
	
	//check if dbrm data resides in older directory path and inform user if it does
	dbrmDirCheck();

	if ( IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM ) {
		//run the mysql / mysqld setup scripts
		cout << endl << "===== Running the InfiniDB MySQL setup scripts =====" << endl << endl;

		system("netstat -na | grep 3306 | grep LISTEN > /tmp/mysqlport");
		string fileName = "/tmp/mysqlport";
		ifstream oldFile (fileName.c_str());
		if (oldFile) {
			oldFile.seekg(0, std::ios::end);
			int size = oldFile.tellg();
			if ( size != 0 ) {
				cout << "The mysqld default port of 3306 is already in-use by another mysqld" << endl;
				cout << "Either change the port number of 3306 in /usr/local/Calpont/mysql/my.cnf" << endl;
				cout << "Or stop current mysqld version that is running" << endl;

				while(true)
				{
					string answer = "n";
					pcommand = callReadline("Enter 'y' when a change has been made and you are ready to continue > ");
					if (pcommand)
					{
						if (strlen(pcommand) > 0) answer = pcommand;
						callFree(pcommand);
						pcommand = 0;
					}
					if ( answer == "y" )
						break;
					else
						cout << "Invalid Entry, please enter 'y' for yes" << endl;
					if ( noPrompting )
						exit(1);
				}
			}
		}

		// call the mysql setup scripts
		mysqlSetup();
		sleep(5);
	}

	if ( IserverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM || 
			pmNumber > 1 ) {

		//
		// perform remote install of other servers in the system
		//
		cout << endl << "===== System Server Installation =====" << endl << endl;
	
		string install = "y";
		cout << "System Configuration is complete, System Server Installation is the next step." << endl;
		cout << "The Calpont InfiniDB Package will be distributed and installed on all of the other system servers." << endl << endl;

		while(true)
		{
			pcommand = callReadline("Would you like to continue with the System Installation? [y,n] (y)  > ");
			if (pcommand)
			{
				if (strlen(pcommand) > 0) install = pcommand;
				callFree(pcommand);
				pcommand = 0;
			}
			if ( install == "y" || install == "n" )
				break;
			else
				cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
			install = "y";
			if ( noPrompting )
				exit(1);
		}
	
		if ( install == "y" ) {
	
			SystemSoftware systemsoftware;
		
			try
			{
				oam.getSystemSoftware(systemsoftware);
			}
			catch (exception& e)
			{
				cout << " ERROR: reading getSystemSoftware API" << endl;
				exit (-1);
			}

			cout << endl;

			//Write out Updated System Configuration File
			string EEPackageType;
			try {
				EEPackageType = sysConfig->getConfig(InstallSection, "EEPackageType");
			}
			catch(...)
			{
				cout << "ERROR: Problem getting EEPackageType from the InfiniDB System Configuration file" << endl;
				exit(1);
			}

			while(true) {
				prompt = "Enter the Package Type being installed to other servers [rpm,deb,binary] (" + EEPackageType + ") > ";
				pcommand = callReadline(prompt);
				if (pcommand) {
					if (strlen(pcommand) > 0) EEPackageType = pcommand;
					callFree(pcommand);
					pcommand = 0;
				}

				if ( EEPackageType == "rpm" || EEPackageType == "deb" || EEPackageType == "binary"  ) {
					break;
				}
				cout << "Invalid Package Type, please re-enter" << endl;
				EEPackageType = "rpm";
				if ( noPrompting )
					exit(1);
			}

			if ( EEPackageType == "rpm" )
				cout << "Performing an InfiniDB System install using RPM packages located in the /root/ directory." << endl;
			else
			{
				if ( EEPackageType == "binary" )
					cout << "Performing an InfiniDB System install using a Binary package located in the /root/ directory." << endl;
				else
					cout << "Performing an InfiniDB System install using using DEB packages located in the /root/ directory." << endl;
			}
	
			//Write out Updated System Configuration File
			try {
				sysConfig->setConfig(InstallSection, "EEPackageType", EEPackageType);
			}
			catch(...)
			{
				cout << "ERROR: Problem setting EEPackageType from the InfiniDB System Configuration file" << endl;
				exit(1);
			}
		
			if ( !writeConfig(sysConfig) ) { 
				cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl; 
				exit(1);
			}


			//check if pkgs are located in /root directory
			if ( EEPackageType != "binary") {
				string seperator = "-";
				if ( EEPackageType == "deb" )
					seperator = "_";
				calpontPackage = "calpont" + seperator + systemsoftware.Version + "-" + systemsoftware.Release;
				mysqlPackage = "calpont-mysql" + seperator + systemsoftware.Version + "-" + systemsoftware.Release;
				mysqldPackage = "calpont-mysqld" + seperator + systemsoftware.Version + "-" + systemsoftware.Release;

				if( !pkgCheck() ) {
					exit(1);
				}
				else
				{
					calpontPackage = "/root/" + calpontPackage + "*." + EEPackageType;
					mysqlPackage = "/root/" + mysqlPackage  + "*." + EEPackageType;
					mysqldPackage = "/root/" + mysqldPackage  + "*." + EEPackageType;
				}
			}
			else
			{
				calpontPackage = "calpont-infinidb-ent-" + systemsoftware.Version + "-" + systemsoftware.Release;
				mysqlPackage = calpontPackage;
				mysqldPackage = calpontPackage;

				if( !pkgCheck() )
					exit(1);
				calpontPackage = "/root/" + calpontPackage + "*.bin.tar.gz";;
			}

			cout << endl;
			cout << "Next step is to enter the password to access the other Servers." << endl;
			cout << "This is either the root password or you can default to default to use a ssh key" << endl;
			cout << "If using the root password, the password needs to be the same on all Servers." << endl << endl;


			while(true)
			{	
				char  *pass1, *pass2;

				if ( noPrompting ) {
					cout << "Enter the 'root' password, hit 'enter' to default to using a ssh key, or 'exit' > " << endl;
					if ( password.empty())
						password = "ssh";
					break;
				}

				pass1=getpass("Enter the 'root' password, hit 'enter' to default to using a ssh key, or 'exit' > ");
				if ( strcmp(pass1, "") == 0 ) {
					password = "ssh";
					break;
				}

				if ( strcmp(pass1, "exit") == 0 )
					exit(0);
				string p1 = pass1;
				pass2=getpass("Confirm password > ");
				string p2 = pass2;
				if ( p1 == p2 ) {
					password = p2;
					break;
				}
				else
					cout << "Password mismatch, please re-enter" << endl;
			}

			//add single quote for special characters
			if ( password != "ssh" )
			{
				password = "'" + password + "'";
			}

			ChildModuleList::iterator list1 = childmodulelist.begin();
			for (; list1 != childmodulelist.end() ; list1++)
			{
				string remoteModuleName = (*list1).moduleName;
				string remoteModuleIP = (*list1).moduleIP;
				string remoteHostName = (*list1).hostName;
				string remoteModuleType = remoteModuleName.substr(0,MAX_MODULE_TYPE_SIZE);

				string debug_logfile = remote_installer_debug;
				string logfile;
				if ( remote_installer_debug == "1" ) {
					logfile = "/var/log/Calpont/" + remoteModuleName + "_" + EEPackageType + "_install.log";
					debug_logfile = debug_logfile + " > " + logfile;
				}

				if ( remoteModuleType == "um" ||
					(remoteModuleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM) )
				{
					cout << endl << "----- Performing Install on '" + remoteModuleName + " / " + remoteHostName + "' -----" << endl << endl;

					if ( remote_installer_debug == "1" )
						cout << "Please wait, install log file is located here: " + logfile << endl << endl;

					if ( EEPackageType != "binary" ) {
						string temppwprompt = pwprompt;
						if ( pwprompt == " " )
							temppwprompt = "none";
						
						//run remote installer script
						string cmd = "/usr/local/Calpont/bin/user_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage + " " + mysqlPackage + " " + mysqldPackage + " initial " + EEPackageType + " " + nodeps + " " + temppwprompt + " " +  debug_logfile ;
						int rtnCode = system(cmd.c_str());
						if (rtnCode != 0) {
							cout << endl << "Error returned from user_installer.sh" << endl;
							exit(1);
						}

						//check for mysql password on remote UM
						if ( pwprompt == " " ) {
							string cmd = "/usr/local/Calpont/bin/remote_command.sh " + remoteModuleIP + " " + password + " '/etc/init.d/mysql-Calpont start'";
							int rtnCode = system(cmd.c_str());
							if (rtnCode != 0) {
								cout << endl << "Error returned from mysql-Calpont start" << endl;
								exit(1);
							}

							string prompt = " *** Enter MySQL password > ";
							string mysqlpw = " ";
							for (;;)
							{
								cmd = "/usr/local/Calpont/bin/remote_command.sh " + remoteModuleIP + " " + password + " '/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root " + pwprompt + " -e status' 1 > /tmp/idbmysql.log 2>&1";
								rtnCode = system(cmd.c_str());
								if (rtnCode != 0) {
									cout << endl << "Error returned from remote_command.sh" << endl;
									exit(1);
								}
	
								if (oam.checkLogStatus("/tmp/idbmysql.log", "ERROR 1045") ) {

									if ( prompt == " *** Enter MySQL password > " )
										cout << endl << " MySQL password set on Module '" + remoteModuleName + "', Additional MySQL Install steps being performed" << endl << endl;

									mysqlpw = getpass(prompt.c_str());
									pwprompt = "--password=" + mysqlpw;
									prompt = " *** Password incorrect, please re-enter MySQL password > ";
								}
								else
								{
									if (!oam.checkLogStatus("/tmp/idbmysql.log", "InfiniDB") ) {
										cout << endl << "ERROR: MySQL runtime error, exit..." << endl << endl;
										system("cat /tmp/idbmysql.log");
										exit (1);
									}
									else
									{
										cout << endl << "Additional MySQL Installation steps Successfully Completed on '" + remoteModuleName + "'" << endl << endl;

										cmd = "/usr/local/Calpont/bin/remote_command.sh " + remoteModuleIP + " " + password + " '/etc/init.d/mysql-Calpont stop'";
										int rtnCode = system(cmd.c_str());
										if (rtnCode != 0) {
											cout << endl << "Error returned from mysql-Calpont stop" << endl;
											exit(1);
										}
										unlink("/tmp/idbmysql.log");
										break;
									}
								}
							}

							//re-run post-mysqld-install with password
							cmd = "/usr/local/Calpont/bin/remote_command.sh " + remoteModuleIP + " " + password + "  /usr/local/Calpont/bin/post-mysql-install " + pwprompt;
							rtnCode = system(cmd.c_str());
							if (rtnCode != 0) {
								cout << endl << "Error returned from post-mysql-install" << endl;
								exit(1);
							}
						}
					}
					else
					{	// do a binary package install
						string cmd = "/usr/local/Calpont/bin/binary_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage + " " + remoteModuleType + " initial " + EEPackageType + " " +  serverTypeInstall + " " + debug_logfile;
						int rtnCode = system(cmd.c_str());
						if (rtnCode == 0)
							cout << "Installation Successfully Completed" << endl;
						else
						{
							cout << "Installation Failed: check " << logfile << endl;
							exit(1);
						}
					}
				}
				else
				{
					if (remoteModuleType == "pm" && IserverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM)
					{
						cout << endl << "----- Performing Install on '" + remoteModuleName + " / " + remoteHostName + "' -----" << endl << endl;

						if ( remote_installer_debug == "1" )
							cout << "Please wait, install log file is located here: " + logfile << endl << endl;

						if ( EEPackageType != "binary" ) {
							//run remote installer script
							string cmd = "/usr/local/Calpont/bin/performance_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage + " " + mysqlPackage + " " + mysqldPackage + " initial " + EEPackageType + " " + nodeps + " " + debug_logfile;
							int rtnCode = system(cmd.c_str());
							if (rtnCode != 0) {
								cout << endl << "Error returned from performance_installer.sh" << endl;
								exit(1);
							}
						}
						else	
						{	// do a binary package install
							string cmd = "/usr/local/Calpont/bin/binary_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpontPackage + " " + remoteModuleType + " initial " +  serverTypeInstall + " " + debug_logfile;
							int rtnCode = system(cmd.c_str());
							if (rtnCode == 0)
								cout << "Installation Successfully Completed" << endl;
							else
							{
								cout << "Installation Failed: check " << logfile << endl;
								exit(1);
							}
						}
					}
				}
			}
		}
	}

	//check if local InfiniDB system logging is working
	cout << endl << "===== Checking InfiniDB System Logging Functionality =====" << endl << endl;

	system("/usr/local/Calpont/bin/syslogSetup.sh install >/dev/null 2>&1");
	int ret = system("/usr/local/Calpont/bin/syslogSetup.sh check >/dev/null 2>&1");
	if ( ret != 0)
		cerr << "WARNING: The InfiniDB system logging not correctly setup and working" << endl;
	else
		cout << "The InfiniDB system logging is setup and working on local server" << endl;

	cout << endl << "InfiniDB System Configuration and Installation is Completed" << endl;

	//
	// startup infinidb
	//

	if ( IserverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM || pmNumber > 1 )
	{
		//
		// perform InfiniDB system startup
		//
		cout << endl << "===== Infinidb System Startup =====" << endl << endl;
	
		string start = "y";
		cout << "System Installation is complete. If any part of the install failed," << endl;
		cout << "the problem should be investigated and resolved before continuing." << endl << endl;
		while(true)
		{
			pcommand = callReadline("Would you like to startup the InfiniDB System? [y,n] (y) > ");
			if (pcommand)
			{
				if (strlen(pcommand) > 0) start = pcommand;
				callFree(pcommand);
				pcommand = 0;
			}
			if ( start == "y" || start == "n" )
				break;
			else
				cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
			start = "y";
			if ( noPrompting )
				exit(1);
		}
	
		if ( start == "y" ) {
	
			if ( password.empty() ) {
				while(true)
				{
					cout << endl;
					pcommand = callReadline("Enter the Server 'root' password > ");
					if (pcommand) {
						password = pcommand;
						callFree(pcommand);
						pcommand = 0;
						break;
					}
					if ( noPrompting )
						exit(1);
				}
			}
	
			ChildModuleList::iterator list1 = childmodulelist.begin();
		
			for (; list1 != childmodulelist.end() ; list1++)
			{
				string remoteModuleName = (*list1).moduleName;
				string remoteModuleIP = (*list1).moduleIP;
				string remoteHostName = (*list1).hostName;
		
				//run remote command script
				cout << endl << "----- Starting InfiniDB on '" + remoteModuleName + "' -----" << endl << endl;
				string cmd = "/usr/local/Calpont/bin/remote_command.sh " + remoteModuleIP + " " + password + " '/etc/init.d/infinidb restart' " +  remote_installer_debug;
				int rtnCode = system(cmd.c_str());
				if (rtnCode != 0)
					cout << "Error with running remote_command.sh" << endl;
				else
					cout << "InfiniDB successfully started" << endl;
			}
	
			//start InfiniDB on local server
			cout << endl << "----- Starting InfiniDB on local server -----" << endl << endl;
			int rtnCode = system("/etc/init.d/infinidb start");
			if (rtnCode != 0) {
				cout << "Error Starting InfiniDB local module" << endl;
				cout << "Installation Failed, exiting" << endl;
				exit (-1);
			}
			else
				cout << "InfiniDB successfully started" << endl;
		}
		else
		{
			cout << endl << "You choose not to Start the InfiniDB Software at this time." << endl;
			exit (1);
		}
	}
	else // Single Server start
	{
		cout << endl << "===== InfiniDB System Startup =====" << endl << endl;
	
		string start = "y";
		cout << "System Installation is complete." << endl;
		cout << "If an error occurred while running the InfiniDB MySQL setup scripts," << endl;
		cout << "this will need to be corrected and postConfigure will need to be re-run." << endl << endl;
		while(true)
		{
			pcommand = callReadline("Would you like to startup the InfiniDB System? [y,n] (y) > ");
			if (pcommand)
			{
				if (strlen(pcommand) > 0) start = pcommand;
				callFree(pcommand);
				pcommand = 0;
			}
			if ( start == "y" || start == "n" )
				break;
			else
				cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
			start = "y";
			if ( noPrompting )
				exit(1);
		}
	
		if ( start == "y" ) {
			//start InfiniDB on local server
			cout << endl << "----- Starting InfiniDB on local Server '" + parentOAMModuleName + "' -----" << endl << endl;
			int rtnCode = system("/etc/init.d/infinidb start");
			if (rtnCode != 0) {
				cout << "Error Starting InfiniDB local module" << endl;
				cout << "Installation Failed, exiting" << endl;
				exit (-1);
			}
			else
				cout << endl << "InfiniDB successfully started" << endl;
		}
		else
		{
			cout << endl << "You choose not to Start the InfiniDB Software at this time." << endl;
			exit (1);
		}
	}

	cout << endl << "InfiniDB Database Platform Starting, please wait .";
	cout.flush();
	
	if ( waitForActive() ) {
		cout << " DONE" << endl;

		system("/usr/local/Calpont/bin/dbbuilder 7 > /tmp/dbbuilder.log");

		if (oam.checkLogStatus("/tmp/dbbuilder.log", "System Catalog created") )
			cout << endl << "System Catalog Successfully Created" << endl;
		else
		{
			if ( oam.checkLogStatus("/tmp/dbbuilder.log", "System catalog appears to exist") ) {

				cout << endl << "Run Upgrade Script..";
				cout.flush();

				//send message to procmon's to run upgrade script
				int status = sendUpgradeRequest(IserverTypeInstall);
	
				if ( status != 0 ) {
					cout << "ERROR: Error return in running the upgrade script, check /tmp/upgrade.log" << endl;
					cout << endl << "InfiniDB Install Failed" << endl << endl;
					exit(1);
				}
				else
					cout << " DONE" << endl;
			}
			else
			{
				cout << endl << "System Catalog Create Failure" << endl;
				cout << "Check latest log file in /tmp/dbbuilder.log.*" << endl;
				exit (1);
			}
		}

		cout << endl << "InfiniDB Install Successfully Completed, System is Active" << endl << endl;

		cout << "Enter the following command to define InfiniDB Alias Commands" << endl << endl;

		cout << ". /usr/local/Calpont/bin/calpontAlias" << endl << endl;

		cout << "Enter 'idbmysql' to access the InfiniDB MySQL console" << endl;
		cout << "Enter 'cc' to access the InfiniDB OAM console" << endl << endl;

		cout << flush;
	}
	else
	{
		cout << " FAILED" << endl;
		cout << endl << "InfiniDB System failed to start, check log files in /var/log/Calpont" << endl;
	}
}


/*
 * Setup OS Files by appending the Calpont versions
 */

// /etc OS Files to be updated
string files[] = {
	"exports",
	"fstab",
	"rc.local",
	" "
};

bool setOSFiles(string parentOAMModuleName, int serverTypeInstall, string sharedNothing, string DBRootStorageType)
{
	bool allfound = true;

	//update /etc files
	for ( int i=0;;++i)
	{
		if ( files[i] == " ")
			//end of list
			break;

		//create or update date on file to make sure on exist
		if ( files[i] == "exports") {
			string cmd = "touch /usr/local/Calpont/local/etc/" + parentOAMModuleName + "/exports.calpont"; 
			system(cmd.c_str());
		}

		//create or update date on file to make sure on exist
		if ( files[i] == "rc.local") {
			string cmd = "touch /usr/local/Calpont/local/etc/" + parentOAMModuleName + "/rc.local.calpont"; 
			system(cmd.c_str());
		}

		string fileName = "/etc/" + files[i];

		ifstream oldFile (fileName.c_str());
		if (!oldFile) {
			if ( files[i] == "exports" ) {
				string cmd = "touch " + fileName;
				system(cmd.c_str());
			}
		}

		//make a backup copy before changing
		string cmd = "rm -f " + fileName + ".calpontSave";
		system(cmd.c_str());

		cmd = "cp " + fileName + " " + fileName + ".calpontSave > /dev/null 2>&1";
		system(cmd.c_str());

		//if fstab, cleanup any calpont entries before added new ones
		if ( files[i] == "fstab" && DBRootStorageType == "storage") {
			cleanupFstab();
		}

		cmd = "cat /usr/local/Calpont/local/etc/" + parentOAMModuleName + "/" + files[i] + ".calpont >> " + fileName; 
		int rtnCode = system(cmd.c_str());
		if (rtnCode != 0 && files[i] != "exports")
			cout << "Error Updating " + files[i] << endl;

		cmd = "rm -f /usr/local/Calpont/local/ " + files[i] + "*.calpont > /dev/null 2>&1";
		system(cmd.c_str());

		cmd = "cp /usr/local/Calpont/local/etc/" + parentOAMModuleName + "/" + files[i] + ".calpont /usr/local/Calpont/local/. > /dev/null 2>&1"; 
		system(cmd.c_str());
	}

	return allfound;
}

/*
 * Updated snmpdx.conf with parentOAMModuleIPAddr
 */
bool updateSNMPD(string parentOAMModuleIPAddr)
{
	string fileName = "/usr/local/Calpont/etc/snmpd.conf";

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return true;
	
	vector <string> lines;
	char line[200];
	string buf;
	string newLine;
	string newLine1;
	while (oldFile.getline(line, 200))
	{
		buf = line;
		string::size_type pos = buf.find("parentOAMIPStub",0);
		if (pos != string::npos)
		{
			newLine = buf.substr(0, pos);
    	    newLine.append(parentOAMModuleIPAddr);

			newLine1 = buf.substr(pos+15, 200);
			newLine.append(newLine1);

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
	return true;
}


/*
 * setup Single Server fstab mount file
 */

bool setParentfstab(string UserStorageLoc, string UserStorageType, Devices devices, string DBRootStorageType, string parentOAMModuleName, string parentOAMModuleIPAddr, int serverTypeInstall, string sharedNothing, ChildModuleList childmodulelist)
{
	Oam oam;
	Config* sysConfig = Config::makeConfig();
	string SystemSection = "SystemConfig";

	//update local fstab
	string fileName = "/usr/local/Calpont/local/etc/" + parentOAMModuleName + "/fstab.calpont";

	vector <string> lines;

	string fstab1;
	string fstab2;

	// UM MOUNT SETUP
	if ( serverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM ||
			serverTypeInstall == oam::INSTALL_COMBINE_DM_UM ) {

		if ( UserStorageType == "storage" ) {
			string TempDiskPath;
			oam.getSystemConfig("TempDiskPath", TempDiskPath);
			fstab2 = UserStorageLoc +  " " + TempDiskPath + " ext2 defaults 0 0";
			lines.push_back(fstab2);
		}
	}

	if (DBRootStorageType != "local" ) {
		if ( DBRootStorageType == "storage" )
			DBRootStorageType = "ext2";

		int id=1;
		Devices::iterator list1 = devices.begin();
		for (; list1 != devices.end() ; list1++)
		{
			string DBrootDIR;
			string DBrootID = "DBRoot" + oam.itoa(id);
			try {
				DBrootDIR = sysConfig->getConfig(SystemSection, DBrootID);
			}
			catch(...)
			{
				cout << "ERROR: Problem getting DBRoot in the InfiniDB System Configuration file" << endl;
				exit(1);
			}
			
			string fstab1= *list1 + " " + DBrootDIR + + " " + DBRootStorageType + " ro,noatime,nodiratime,sync,dirsync 0 0";
			lines.push_back(fstab1);
			id++;
		}
	}
	else
	{
		// setup shared-nothing nfs mounts
		if ( sharedNothing == "y" ) {

			ChildModuleList::iterator list1 = childmodulelist.begin();
			for (; list1 != childmodulelist.end() ; list1++)
			{
				if ( (*list1).moduleName.substr(0,MAX_MODULE_TYPE_SIZE) == "pm" ) {
					if ( (*list1).moduleName != parentOAMModuleName) {
						string moduleID = (*list1).moduleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);
						string dbroot = "/usr/local/Calpont/data" + moduleID;
						string fstab1 = (*list1).moduleIP + ":" + dbroot + " " + dbroot + " nfs rw,wsize=8192,rsize=8192,noatime,nodiratime";

						lines.push_back(fstab1);
					}
				}
			}
		}
	}

	unlink (fileName.c_str());
	ofstream newFile (fileName.c_str());	
	
	//create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	close(fd);

	return true;
}


/*
 * setup PM Child Module export file
 */
bool setPMexports(string moduleName, string parentOAMModuleIPAddr )
{
	string moduleID = moduleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE);

	//
	//update exports in /usr/local/Calpont/local/etc/"moduleName"
	//
	string fileName = "/usr/local/Calpont/local/etc/" + moduleName + "/exports.calpont";

	string cmd = "touch " + fileName;
	system(cmd.c_str());

	cmd = "echo '/usr/local/Calpont/data" + moduleID + " " + parentOAMModuleIPAddr + "(rw,sync)' >> " + fileName;
	system(cmd.c_str());

	return true;

}


/*
 * setup User Module fstab mount files
 */
bool setUMfstab(string IPAddress, string moduleName, string UserStorageLoc, string UserStorageType)
{
	Oam oam;

	//
	// update fstab in /usr/local/Calpont/local/etc
	//
	string fileName = "/usr/local/Calpont/local/etc/" + moduleName + "/fstab.calpont";

	vector <string> lines;

	string TempDiskPath;
	oam.getSystemConfig("TempDiskPath", TempDiskPath);

	string fstab6= UserStorageLoc + " " + TempDiskPath + " ext2 defaults 0 0";

	if ( UserStorageType == "storage" )
		lines.push_back(fstab6);

	unlink (fileName.c_str());
	ofstream newFile (fileName.c_str());	
	
	//create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	close(fd);

	return true;
}

/*
 * setup Performance Module fstab mount file
 */
bool setPMfstab(string IPAddress, Devices devices, string mountType, string moduleName)
{
	Oam oam;
	Config* sysConfig = Config::makeConfig();
	string SystemSection = "SystemConfig";

	//update fstab in /usr/local/Calpont/local/etc
	string fileName = "/usr/local/Calpont/local/etc/" + moduleName + "/fstab.calpont";

	vector <string> lines;

	if (mountType != "local" ) {

		if ( mountType == "storage" )
			mountType = "ext2";

		int id=1;
		Devices::iterator list1 = devices.begin();
		for (; list1 != devices.end() ; list1++)
		{
			string DBrootDIR;
			string DBrootID = "DBRoot" + oam.itoa(id);
			try {
				DBrootDIR = sysConfig->getConfig(SystemSection, DBrootID);
			}
			catch(...)
			{
				cout << "ERROR: Problem getting DBRoot in the InfiniDB System Configuration file" << endl;
				exit(1);
			}
			
			string fstab1= *list1 + " " + DBrootDIR + + " " + mountType + " ro,noatime,nodiratime,sync,dirsync 0 0";
			lines.push_back(fstab1);
			id++;
		}
	}

	unlink (fileName.c_str());
	ofstream newFile (fileName.c_str());	
	
	//create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	close(fd);

	return true;
}

/*
 * Update ProcessConfig.xml file for a single server configuration
 * Change the 'um' and 'pm' to 'dm'
 */
bool updateProcessConfig(int serverTypeInstall)
{
	vector <string> oldModule;
	string newModule;

	switch ( serverTypeInstall ) {
		case (oam::INSTALL_COMBINE_DM_UM_PM):
		{
			newModule = ">pm";
			oldModule.push_back(">um");
			oldModule.push_back(">dm");
			break;
		}
		case (oam::INSTALL_COMBINE_DM_UM):
		{
			newModule = ">um";
			oldModule.push_back(">dm");
			break;
		}
		case (oam::INSTALL_COMBINE_PM_UM):
		{
			newModule = ">pm";
			oldModule.push_back(">um");
			break;
		}
	}

	string fileName = "/usr/local/Calpont/etc/ProcessConfig.xml";

	//Save a copy of the original version
	string cmd = "/bin/cp -f " + fileName + " " + fileName + ".calpontSave > /dev/null 2>&1";
	system(cmd.c_str());

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return true;
	
	vector <string> lines;
	char line[200];
	string buf;
	string newLine;
	string newLine1;

	while (oldFile.getline(line, 200))
	{
		buf = line;
		newLine = line;

		std::vector<std::string>::iterator pt1 = oldModule.begin();
		for( ; pt1 != oldModule.end() ; pt1++)
		{
			int start = 0;
			while(true)
			{
				string::size_type pos = buf.find(*pt1,start);
				if (pos != string::npos)
				{
					newLine = buf.substr(0, pos);
					newLine.append(newModule);
		
					newLine1 = buf.substr(pos+3, 200);
					newLine.append(newLine1);
					start = pos+3;
				}
				else
				{
					buf = newLine;
					start = 0;
					break;
				}
			}
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
	return true;
}

/*
 * Uncomment entry in Calpont.xml
 */
bool uncommentCalpontXml( string entry)
{
	string fileName = "/usr/local/Calpont/etc/Calpont.xml";

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return true;
	
	vector <string> lines;
	char line[200];
	string buf;
	string newLine;

	string firstComment = "<!--";
	string lastComment = "--> ";

	while (oldFile.getline(line, 200))
	{
		buf = line;

		string::size_type pos = buf.find(entry,0);
		if (pos != string::npos)
		{
			pos = buf.find(firstComment,0);
			if (pos == string::npos)
			{
				return true;
			}
			else
			{
				buf = buf.substr(pos+4,80);

				pos = buf.find(lastComment,0);
				if (pos == string::npos)
				{
					return true;
				}
				else
				{
					buf = buf.substr(0,pos);
				}
			}
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
	return true;
}

/*
 * Make makeRClocal to set mount scheduler
 */
bool makeRClocal(string moduleType, string moduleName, int IserverTypeInstall)
{
	string fileName = "/usr/local/Calpont/local/etc/" + moduleName + "/rc.local.calpont";

	vector <string> lines;

	string mount1;
	string mount2
;
	switch ( IserverTypeInstall ) {
		case (oam::INSTALL_NORMAL):	// normal
		{
			if ( moduleType == "um" )
				mount1 = "/mnt\\/tmp/";
			else
				if ( moduleType == "pm" )
					mount1 = "/Calpont\\/data/";
				else
					return true;
			break;
		}
		case (oam::INSTALL_COMBINE_DM_UM_PM):	// combined #1 - dm/um/pm
		{
			if ( moduleType == "pm" ) {
				mount1 = "/mnt\\/tmp/";
				mount2 = "/Calpont\\/data/";
			}
			else
				return true;
			break;
		}
		case (oam::INSTALL_COMBINE_DM_UM):	// combined #2 dm/um on a same server
		{
			if ( moduleType == "um" )
				mount1 = "/mnt\\/tmp/";
			else
				if ( moduleType == "pm" )
					mount1 = "/Calpont\\/data/";
				else
					return true;
			break;
		}
		case (oam::INSTALL_COMBINE_PM_UM):	// combined #3 um/pm on a same server
		{
			if ( moduleType == "pm" ) {
				mount1 = "/mnt\\/tmp/";
				mount2 = "/Calpont\\/data/";
			}
			else
				return true;
			break;
		}
	}

	if ( !mount1.empty() ) {
		string line1 = "for scsi_dev in `mount | awk '" + mount1 + " {print $1}' | awk -F/ '{print $3}' | sed 's/[0-9]*$//'`; do";
		string line2 = "echo deadline > /sys/block/$scsi_dev/queue/scheduler";
		string line3 = "done";
	
		lines.push_back(line1);
		lines.push_back(line2);
		lines.push_back(line3);
	}
	else
	{
		if ( !mount2.empty() ) {
			string line1 = "for scsi_dev in `mount | awk '" + mount2 + " {print $1}' | awk -F/ '{print $3}' | sed 's/[0-9]*$//'`; do";
			string line2 = "echo deadline > /sys/block/$scsi_dev/queue/scheduler";
			string line3 = "done";
		
			lines.push_back(line1);
			lines.push_back(line2);
			lines.push_back(line3);
		}
	}

	unlink (fileName.c_str());

	if ( lines.begin() == lines.end()) {
		string cmd = "touch " + fileName;
		system(cmd.c_str());
		return true;
	}

   	ofstream newFile (fileName.c_str());	

	//create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	close(fd);

	return true;
}

/*
 * createDbrootDirs 
 */
bool createDbrootDirs(string DBRootStorageType)
{
	int rtnCode;

	// mount disk and create directories if configured with storage
	if ( DBRootStorageType == "storage" ) {
		system("/etc/init.d/nfs stop > /dev/null 2>&1");
		system("/etc/init.d/syslog stop > /dev/null 2>&1");
		system("/etc/init.d/syslog-ng stop > /dev/null 2>&1");
		system("/etc/init.d/rsyslog stop > /dev/null 2>&1");
		system("mount -al  > /dev/null 2>&1");

		rtnCode = system("mount /usr/local/Calpont/data1 -o rw,remount");
		if (rtnCode != 0) {
			cout << endl << "Error: failed to mount data1 as read-write" << endl;
			return false;
		}

		// create system file directories
	
		rtnCode = system("mkdir -p /usr/local/Calpont/data1/systemFiles/dbrm > /dev/null 2>&1");
		if (rtnCode != 0) {
			cout << endl << "Error: failed to make mount dbrm dir" << endl;
			return false;
		}
	
		rtnCode = system("mkdir -p /usr/local/Calpont/data1/systemFiles/dataTransaction/archive > /dev/null 2>&1");
		if (rtnCode != 0) {
			cout << endl << "Error: failed to make mount dataTransaction dir" << endl;
			return false;
		}

		system("/etc/init.d/nfs start > /dev/null 2>&1");
		system("/etc/init.d/syslog start > /dev/null 2>&1");
		system("/etc/init.d/syslog-ng start > /dev/null 2>&1");
		system("/etc/init.d/rsyslog start > /dev/null 2>&1");
	}

	system("chmod 1777 -R /usr/local/Calpont/data1/systemFiles/dbrm > /dev/null 2>&1");

	return true;
}


/*
 * writeConfig 
 */
bool writeConfig( Config* sysConfig ) 
{
	for ( int i = 0 ; i < 3 ; i++ )
	{
		try {
			sysConfig->write();
			return true;
		}
		catch(...)
		{}
	}

	return false;
}
	
/*
 * pkgCheck 
 */
bool pkgCheck()
{
	while(true) 
	{
		string cmd = "ls /root/. | grep " + calpontPackage + " > /tmp/calpontpkgs";
		system(cmd.c_str());
	
		cmd = "ls /root/. | grep " + mysqlPackage + " > /tmp/mysqlpkgs";
		system(cmd.c_str());
	
		cmd = "ls /root/. | grep " + mysqldPackage + " > /tmp/mysqldpkgs";
		system(cmd.c_str());

		string pkg = calpontPackage;
		string fileName = "/tmp/calpontpkgs";
		ifstream oldFile (fileName.c_str());
		if (oldFile) {
			oldFile.seekg(0, std::ios::end);
			int size = oldFile.tellg();
			if ( size != 0 ) {
				oldFile.close();
				unlink (fileName.c_str());
	
				pkg = mysqlPackage;
				fileName = "/tmp/mysqlpkgs";
				ifstream oldFile1 (fileName.c_str());
				if (oldFile1) {
					oldFile1.seekg(0, std::ios::end);
					size = oldFile1.tellg();
					if ( size != 0 ) {
						oldFile1.close();
						unlink (fileName.c_str());
		
						pkg = mysqldPackage;
						fileName = "/tmp/mysqldpkgs";
						ifstream oldFile2 (fileName.c_str());
						if (oldFile2) {
							oldFile2.seekg(0, std::ios::end);
							size = oldFile2.tellg();
							if ( size != 0 ) {
								oldFile2.close();
								unlink (fileName.c_str());
								// all 3 pkgs found
								return true;
							}
						}
					}
				}
			}
		}
	
		cout << endl << " Error: can't locate " + pkg + " Package in the /root/ directory" << endl << endl;
		if (noPrompting)
			exit (1);
	
		while(true)
		{
			pcommand = callReadline("Please place a copy of the InfiniDB Packages in the /root/ directory and press <enter> to continue or enter 'exit' to exit the install > ");
			if (pcommand) {
				if (strcmp(pcommand, "exit") == 0)
				{
					callFree(pcommand);
					pcommand = 0;
					return false;
				}
				if (strlen(pcommand) == 0)
				{
					callFree(pcommand);
					pcommand = 0;
					break;
				}
				callFree(pcommand);
				pcommand = 0;
				cout << "Invalid entry, please re-enter" << endl;
				if ( noPrompting )
					exit(1);
				continue;
			}
			break;
		}
	}

	return true;
}

bool storageSetup()
{
	Oam oam;

	//
	// get Data storage Mount
	//

	cout << "===== Setup Data Storage Mount Configuration =====" << endl << endl;

	cout << "There are 2 options when configuring the storage: ext2 and local" << endl << endl;
	cout << "  'ext2'     - This is used for ext2 device mounting for the dbroot storages." << endl;
	cout << "               If selected, then the user will need to enter the device name," << endl;
	cout << "               like /dev/sda1, and this device should be setup as a 'ext2' device." << endl << endl;
	cout << "  'local'    - This is used when you are using the local disk for the dbroot storages" << endl;
	cout << "               or when there are softlinks setup from the default storage mounts to" << endl;
	cout << "               a mount directory, like /usr/local/Calpont/data1 softlinked to /mnt/data1." << endl << endl; 

	try {
		sharedNothing = sysConfig->getConfig(InstallSection, "SharedNothing");
		DBRootStorageType = sysConfig->getConfig(InstallSection, "DBRootStorageType");
		DBRootCount = strtol(sysConfig->getConfig(SystemSection, "DBRootCount").c_str(), 0, 0);
	}
	catch(...)
	{
		cout << "ERROR: Problem getting DB Storage Data from the InfiniDB System Configuration file" << endl;
		return false;
	}

	//Shared-Nothing or Shared-Everything configuration
	string shared = "shared-nothing";
	if ( sharedNothing == "n" )
		shared = "shared-everything";

	if (sharedNothingSupport) {
		string newShared = shared;
		while(true)
		{
			prompt = "Enter InfiniDB Data Storage Mount [shared-nothing,shared-everything] (" + shared + ") > ";
			pcommand = callReadline(prompt);
			if (pcommand)
			{
				if (strlen(pcommand) > 0) newShared = pcommand;
				callFree(pcommand);
				pcommand = 0;
			}
			if ( newShared == "shared-nothing" || newShared == "shared-everything" )
				break;
			cout << "Invalid Type, please re-enter" << endl;
			newShared = shared;
			if ( noPrompting )
				exit(1);
		}
	
		sharedNothing = "n";
		if ( newShared == "shared-nothing" )
			sharedNothing = "y";
	
		try {
			sysConfig->setConfig(InstallSection, "SharedNothing", sharedNothing);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting sharedNothing in the InfiniDB System Configuration file" << endl;
			return false;
		}
	}
	else
		sharedNothing = "no";

	//if shared-nothing, set up have DBRoot disk local and ProcessManager running Simplex
	if ( sharedNothing == "y" ) {
		DBRootStorageType = "local";

		try {
			oam.setProcessConfig("ProcessManager", "pm1", "RunType", "SIMPLEX");
		}
		catch(...)
		{
			cout << "ERROR: Problem setting ProcessManager RunType to Simplex in ProcessConfig.xml" << endl;
			return false;
		}
	}
	else
	{
		//setup dbroot storage
		string storageType;

		while(true)
		{
			storageType = "1";
			if ( DBRootStorageType == "local" )
				storageType = "2";

			prompt = "Select the type of Data Storage [1=ext2, 2=local] (" + storageType + ") > ";
			pcommand = callReadline(prompt);
			if (pcommand)
			{
				if (strlen(pcommand) > 0) storageType = pcommand;
				callFree(pcommand);
				pcommand = 0;
			}
			if ( storageType == "1" || storageType == "2")
				break;
			cout << endl << "Invalid Entry, please re-enter" << endl << endl;
			if ( noPrompting )
				exit(1);
		}

		if ( storageType == "1" )
			DBRootStorageType = "storage";	// use for 'ext2'
		else
			DBRootStorageType = "local";

		while(true)
		{
			prompt = "Enter the Number of InfiniDB Data Storage (DBRoots) areas (" + oam.itoa(DBRootCount) + ") > ";
			pcommand = callReadline(prompt);
			if (pcommand) {
				int newDBRootCount = DBRootCount;
				if (strlen(pcommand) > 0) newDBRootCount = atoi(pcommand);
				callFree(pcommand);
				pcommand = 0;
				if (newDBRootCount <= 0) {
					cout << "ERROR: Invalid Number, please reenter" << endl;
					if ( noPrompting )
						exit(1);
					continue;
				}
	
				DBRootCount = newDBRootCount;
				try {
					sysConfig->setConfig(SystemSection, "DBRootCount", oam.itoa(DBRootCount));
				}
				catch(...)
				{
					cout << "ERROR: Problem setting DBRoot Count in the InfiniDB System Configuration file" << endl;
					return false;
				}
			}
			break;
		}
	
		for( int i=1 ; i < DBRootCount+1 ; i++)
		{
			string DBRootStorageLocID = "DBRootStorageLoc" + oam.itoa(i);
			if ( DBRootStorageType != "local") {
				while(true)
				{
					try {
						DBRootStorageLoc = sysConfig->getConfig(InstallSection, DBRootStorageLocID);
					}
					catch(...)
					{
						cout << "ERROR: Problem getting DB Storage Data from the InfiniDB System Configuration file" << endl;
						return false;
					}
			
					prompt = "Enter Storage Location for DBRoot #" + oam.itoa(i) + " (" + DBRootStorageLoc + ") > ";
					pcommand = callReadline(prompt);
					if (pcommand)
					{
						if (strlen(pcommand) > 0) DBRootStorageLoc = pcommand;
						callFree(pcommand);
						pcommand = 0;
					}
					if ( DBRootStorageLoc != oam::UnassignedName )
						break;
					cout << "ERROR: Invalid entry, please re-enter" << endl;
				}

				devices.push_back(DBRootStorageLoc);
		
				try {
					sysConfig->setConfig(InstallSection, DBRootStorageLocID, DBRootStorageLoc);
				}
				catch(...)
				{
					cout << "ERROR: Problem setting DBRootStorageLoc in the InfiniDB System Configuration file" << endl;
					return false;
				}

				//mount data1 incase dbrm files need to be moved
				if ( i == 1 ) {
					string mountcmd = "mount " + DBRootStorageLoc + " /usr/local/Calpont/data1 -o rw > /tmp/mount 2>&1";
					system(mountcmd.c_str());

					//test that data1 is mounted rw
					ofstream fout("/usr/local/Calpont/data1/testmount");
					if (!fout) {
						cout << "ERROR: Problem mounting to data1 storage device of " +  DBRootStorageLoc << endl;
						return false;
					}
					unlink ("/usr/local/Calpont/data1/testmount");
				}
			}
			else
			{
				try {
					sysConfig->setConfig(InstallSection, DBRootStorageLocID, oam::UnassignedName);
				}
				catch(...)
				{
					cout << "ERROR: Problem setting DBRootStorageLoc in the InfiniDB System Configuration file" << endl;
					return false;
				}
			}
	
			string DBrootID = "DBRoot" + oam.itoa(i);
			string pathID =  "/usr/local/Calpont/data" + oam.itoa(i);
	
			try {
				sysConfig->setConfig(SystemSection, DBrootID, pathID);
			}
			catch(...)
			{
				cout << "ERROR: Problem setting DBRoot in the InfiniDB System Configuration file" << endl;
				return false;
			}
	
			//create dbroot directories
			string cmd = "mkdir " + pathID + " > /dev/null 2>&1";
			system(cmd.c_str());
		}

		//clear out any additional dbroots entries
		for ( int i = DBRootCount+1 ; i < 100 ; i++ )
		{
			string DBrootID = "DBRoot" + oam.itoa(i);
	
			try {
				string entry = sysConfig->getConfig(SystemSection, DBrootID);
				if ( !entry.empty())
					sysConfig->setConfig(SystemSection, DBrootID, "unavailable");
				else
					break;
			}
			catch(...)
			{
				break;
			}
			
		}
	}

	try {
		sysConfig->setConfig(InstallSection, "DBRootStorageType", DBRootStorageType);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting DBRootStorageType in the InfiniDB System Configuration file" << endl;
		return false;
	}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		return false;
	}

	return true;
}

void snmpAppCheck()
{
	Oam oam;

	cout << endl << "===== InfiniDB SNMP-Trap Process Check  =====" << endl << endl;
	cout << "InfiniDB is packaged with a SNMP-Trap Process." << endl;
	cout << "If the system where InfiniDB is being installed already has SNMP-Trap Process" << endl;
	cout << "running, then you have the option of disabling InfiniDB's SNMP-Trap Process." << endl;
	cout << "Not having the InfiniDB SNMP_trap Process will effect the" << endl;
	cout << "generation of InfiniDB Alarms and associated SNMP Traps." << endl;
	cout << "Please reference the Calpont InfiniDB Installation Guide for" << endl;
	cout << "addition information." << endl << endl;

	string enableSNMP = "y";
	try {
		enableSNMP = sysConfig->getConfig(InstallSection, "EnableSNMP");
	}
	catch(...)
	{}

	if (enableSNMP.empty() || enableSNMP == "" )
		enableSNMP = "y";

	while(true)
	{
		if ( enableSNMP == "y" ) {
			string disable = "n";
			pcommand = callReadline("InfiniDB SNMP-Trap Process is enabled, would you like to disable it [y,n] (n) > ");
			if (pcommand)
			{
				if (strlen(pcommand) > 0) disable = pcommand;
				callFree(pcommand);
				pcommand = 0;
			}

			if ( disable == "y" ) {
				enableSNMP = "n";
				break;
			}
			else
			{
				if ( disable == "n" ) {
					enableSNMP = "y";
					break;
				}
			}
	
			cout << "Invalid Entry, please retry" << endl;
			if ( noPrompting )
				exit(1);
		}
		else
		{
			string enable = "n";
			pcommand = callReadline("InfiniDB SNMP is disabled, would you like to enable them [y,n] (n) > ");
			if (pcommand)
			{
				if (strlen(pcommand) > 0) enable = pcommand;
				callFree(pcommand);
				pcommand = 0;
			}

			if ( enable == "y" || enable == "n" ) {
				enableSNMP = enable;
				break;
			}

			cout << "Invalid Entry, please retry" << endl;
			if ( noPrompting )
				exit(1);
		}
	}

	try {
		sysConfig->setConfig(InstallSection, "EnableSNMP", enableSNMP);
	}
	catch(...)
	{}

	if (enableSNMP == "y") {
		//
		// Configure SNMP / NMS Addresses
		//
		try
		{
			oam.setProcessConfig("SNMPTrapDaemon", "ParentOAMModule", "BootLaunch", "1");
			oam.setProcessConfig("SNMPTrapDaemon", "ParentOAMModule", "LaunchID", "3");

			cout << endl << "InfiniDB SNMP Process successfully enabled" << endl;
		}
		catch (exception& e)
		{
			cout << endl << "**** setProcessConfig Failed =  " << e.what() << endl;
		}

		//set OAM Parent IP Address in snmpd.conf
		if( !updateSNMPD(parentOAMModuleIPAddr) )
			cout << "updateSNMPD error" << endl;
	
		//get and set NMS IP address
		string currentNMSIPAddress;
		string NMSIPAddress;
		SNMPManager sm;
		sm.getNMSAddr(currentNMSIPAddress);
	
		NMSIPAddress = currentNMSIPAddress;

		cout << endl << "===== Setup the Network Management System (NMS) Server Configuration =====" << endl << endl;

		cout << "This would be used to receive SNMP Traps from InfiniDB, like a Network Control Center" << endl;
		prompt = "Enter IP Address(es) of NMS Server (" + currentNMSIPAddress + ") > ";
		pcommand = callReadline(prompt.c_str());
		if (pcommand)
		{
			if (strlen(pcommand) > 0) NMSIPAddress = pcommand;
			callFree(pcommand);
			pcommand = 0;
		}
	
		sm.setNMSAddr(NMSIPAddress);
	}
	else
	{	//disabled, update config file
		try
		{
			oam.setProcessConfig("SNMPTrapDaemon", "ParentOAMModule", "BootLaunch", "0");
			oam.setProcessConfig("SNMPTrapDaemon", "ParentOAMModule", "LaunchID", "0");

			cout << endl << "InfiniDB SNMP-Trap Process successfully disabled" << endl;
		}
		catch (exception& e)
		{
			cout << endl << "**** setProcessConfig Failed =  " << e.what() << endl;
		}
	}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}

	return;
}

void setSystemName()
{
	cout << endl;

	//setup System Name
	string systemName;
	try {
		systemName = sysConfig->getConfig(SystemSection, "SystemName");
	}
	catch(...)
	{
		systemName = oam::UnassignedName;
	}

	if ( systemName.empty() )
		systemName = oam::UnassignedName;

	prompt = "Enter System Name (" + systemName + ") > ";
	pcommand = callReadline(prompt);
	if (pcommand)
	{
		if (strlen(pcommand) > 0) systemName = pcommand;
		callFree(pcommand);
		pcommand = 0;
	}

	try {
		 sysConfig->setConfig(SystemSection, "SystemName", systemName);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting SystemName from the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	if ( !writeConfig(sysConfig) ) {
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
		exit(1);
	}
}

/*
 * Create a module file
 */
bool makeModuleFile(string moduleName, string parentOAMModuleName)
{
	string fileName;
	if ( moduleName == parentOAMModuleName)
		fileName = "/usr/local/Calpont/local/module";
	else
		fileName = "/usr/local/Calpont/local/etc/" + moduleName + "/module";

	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());

	string cmd = "echo " + moduleName + " > " + fileName;
	system(cmd.c_str());

	newFile.close();

	return true;
}

void offLineAppCheck()
{
	//check for system startup type, process offline or online option
	cout << endl << "===== Setup Process Startup offline Configuration =====" << endl << endl;

	string systemStartupOffline;
	try {
		systemStartupOffline = sysConfig->getConfig(InstallSection, "SystemStartupOffline");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting systemStartupOffline from the InfiniDB System Configuration file" << endl;
		exit(1);
	}

	cout << endl;
	string temp = "y";
	while(true)
	{
		prompt = "Do you want the Database Processes started automatically at system startup [y,n] (y) > ";
		pcommand = callReadline(prompt);
		if (pcommand) {
			if (strlen(pcommand) > 0) temp = pcommand;
			callFree(pcommand);
			pcommand = 0;
			if ( temp == "n" || temp == "y") {
				break;
			}
			cout << "Invalid Option, please re-enter" << endl;
			if ( noPrompting )
				exit(1);
		}
		else
			break;
	}

	if ( temp == "y" )
		systemStartupOffline = "n";
	else
		systemStartupOffline = "y";

	try {
		sysConfig->setConfig(InstallSection, "SystemStartupOffline", systemStartupOffline);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting systemStartupOffline in the InfiniDB System Configuration file" << endl;
		exit(1);
	}
}

/*
 * Check for reuse of RPM saved Calpont.xml
 */
bool checkSaveConfigFile()
{
    char* pcommand = 0;
	string rpmFileName = "/usr/local/Calpont/etc/Calpont.xml";
	string newFileName = "/usr/local/Calpont/etc/Calpont.xml.new";

	string extentMapCheckOnly = " ";

	//check if Calpont.xml.rpmsave exist
	ifstream File (oldFileName.c_str());
	if (!File) {
		if ( noPrompting ) {
			cout << endl << "Old Config File not found '" +  oldFileName + "', exiting" << endl;
			exit(1);
		}
		return true;
	}
	File.close();

	// If 'oldFileName' isn't configured, exit
	Config* oldSysConfig = Config::makeConfig(oldFileName);

	if ( !noPrompting ) {
		string oldExeMgr = oldSysConfig->getConfig("ExeMgr1", "IPAddr");
		if ( oldExeMgr == "0.0.0.0") {
			cout << endl << "Old Config File not Configured, ExeMgr1 entry is '0.0.0.0', '" +  oldFileName + "', exiting" << endl;
			exit(1);
		}
		else
		{	//set default incase it's not set of old Calpont.xml
			if ( oldExeMgr == "127.0.0.1")
				singleServerInstall = "y";
			else
				singleServerInstall = "n";
		}
	}

	// get single-server system install type
	string temp;
	try {
		temp = oldSysConfig->getConfig(InstallSection, "SingleServerInstall");
	}
	catch(...)
	{}

	if ( !temp.empty() )
		singleServerInstall = temp;

	if ( singleServerInstall == "y" )
		singleServerInstall = "1";
	else
		singleServerInstall = "2";

	if ( !noPrompting ) {
		cout << endl << "A copy of the InfiniDB Configuration file has been saved during Package install." << endl;
		if ( singleServerInstall == "1")
			cout << "It's Configured for a Single Server Install." << endl; 
		else
			cout << "It's Configured for a Multi-Server Install." << endl; 
	
		cout << "You have an option of utilizing the configuration data from that file or starting" << endl;
		cout << "with the InfiniDB Configuration File that comes with the InfiniDB Package." << endl;
		cout << "You will only want to utilize the old configuration data when performing the same" << endl;
		cout << "type of install, i.e. Single or Multi-Server" << endl;
	}
	else
	{
		cout << "The Calpont Configuration Data is taken from " << oldFileName << endl;
	}

	cout << endl;
	while(true)
	{
		pcommand = callReadline("Do you want to utilize the configuration data from the saved copy? [y,n]  > ");
		if (pcommand)
		{
			if (strlen(pcommand) > 0)
			{
				reuseConfig = pcommand;
			}
			else
			{
				if ( noPrompting )
					reuseConfig = "y";
				else
				{
					cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
					if ( noPrompting )
						exit(1);
					continue;
				}
			}

			callFree(pcommand);
			pcommand = 0;
		}

		if ( reuseConfig == "y" ) {
			if ( singleServerInstall == "1") {
				system("rm -f /usr/local/Calpont/etc/Calpont.xml.installSave  > /dev/null 2>&1");
				system("mv -f /usr/local/Calpont/etc/Calpont.xml /usr/local/Calpont/etc/Calpont.xml.installSave  > /dev/null 2>&1");
				system("/bin/cp -f /usr/local/Calpont/etc/Calpont.xml.singleserver  /usr/local/Calpont/etc/Calpont.xml  > /dev/null 2>&1");
			}
			break;
		}

		if ( reuseConfig == "n" ) {
			extentMapCheckOnly = "-e";
			break;
		}
		else
			cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
	}

	// clear this entry out to validate updates being made
	Config* sysConfig = Config::makeConfig();
	sysConfig->setConfig("ExeMgr1", "IPAddr", "0.0.0.0");

	for ( int retry = 0 ; retry < 5 ; retry++ )
	{
		string cmd = "mv -f " + rpmFileName + " " + newFileName;
		int rtnCode = system(cmd.c_str());
		if (rtnCode != 0) {
			cout << "Error moving installed version of Calpont.xml" << endl;
			return false;
		}
	
		cmd = "cp " + oldFileName + " " + rpmFileName;
		rtnCode = system(cmd.c_str());
		if (rtnCode != 0) {
			cout << "Error moving pkgsave file" << endl;
			return false;
		}
	
		cmd = "cd /usr/local/Calpont/etc/;../bin/autoConfigure " + extentMapCheckOnly;
		rtnCode = system(cmd.c_str());
		if (rtnCode != 0) {
			cout << "Error running autoConfigure" << endl;
			return false;
		}
	
		cmd = "mv -f " + newFileName + " " + rpmFileName;
		rtnCode = system(cmd.c_str());
		if (rtnCode != 0) {
			cout << "Error moving pkgsave file" << endl;
			return false;
		}

		//check to see if updates were made	
		if ( sysConfig->getConfig("ExeMgr1", "IPAddr") != "0.0.0.0")
			return true;

		sleep(1);
	}

	if ( reuseConfig == "n" )
		return true;

	cout << "ERROR: Failed to copy data to Calpont.xml" << endl;
	return false;

}

