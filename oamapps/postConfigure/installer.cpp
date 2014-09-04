/******************************************************************************************
* $Id: installer.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
* List of files that will be updated during system install time on each server
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
#include "stdio.h"
#include "ctype.h"
#include <netdb.h>
#include <sys/sysinfo.h>

#include "liboamcpp.h"
#include "configcpp.h"
#include "snmpmanager.h"

using namespace std;
using namespace oam;
using namespace config;
using namespace snmpmanager;

#include "helpers.h"
using namespace installer;

typedef struct Module_struct
{
	std::string     moduleName;
	std::string     moduleIP;
	std::string     hostName;
} Module;

typedef std::vector<Module> ModuleList;

bool setOSFiles(string parentOAMModuleName, int serverTypeInstall);
bool makeBASH();
bool makeModuleFile(string moduleName, string parentOAMModuleName);
bool updateProcessConfig(int serverTypeInstall);
bool makeRClocal(string moduleName, int IserverTypeInstall);
bool createDbrootDirs(string DBRootStorageType);
bool uncommentCalpontXml( string entry);
bool writeConfig(Config* sysConfig);

extern string pwprompt;
extern string mysqlpw;

string installDir;

int main(int argc, char *argv[])
{
	string cmd;
    Oam oam;
	string parentOAMModuleIPAddr;
	string parentOAMModuleHostName;
	ModuleList childmodulelist;
	ModuleList directormodulelist;
	ModuleList usermodulelist;
	ModuleList performancemodulelist;
	Module childmodule;

  	struct sysinfo myinfo; 

	string prompt;
	string nodeps = "-h";
	string installer_debug = "0";
	string packageType = "rpm";

	Config* sysConfig = Config::makeConfig();
	string SystemSection = "SystemConfig";
	string InstallSection = "Installation";
	
	string DBRootStorageType;

	if (argc < 10)
	{
		cerr << "installer: ERROR: not enough arguments" << endl;
		exit(1);
	}
	string calpont_rpm = argv[1];
	string mysql_rpm = argv[2];
	string mysqld_rpm = argv[3];
	string installType = argv[4];
	string password = argv[5];
	string reuseConfig = argv[6];
	nodeps = argv[7];
	mysqlpw = argv[8];
	installer_debug = argv[9];
	if (argc >= 11)
		installDir = argv[10];
	else
		installDir = "/usr/local/Calpont";

//	cout << calpont_rpm << endl;
//	cout << mysql_rpm << endl;
//	cout << mysqld_rpm << endl;
//	cout << installType << endl;
//	cout << password << endl;
//	cout << reuseConfig << endl;
//	cout << nodeps << endl;
//	cout << mysqlpw << endl;
//	cout << installer_debug << endl;
//	cout << installDir << endl;
	sleep(1);

	if ( mysqlpw == "dummymysqlpw" )
		mysqlpw = " ";
	pwprompt = "--password=" + mysqlpw;

	//copy Calpont.xml.rpmsave if upgrade option is selected
	if ( installType == "upgrade" ) {
		cmd = "/bin/cp -f " + installDir + "/etc/Calpont.xml " + installDir + "/etc/Calpont.xml.new 2>&1";
		system(cmd.c_str());
		cmd = "/bin/cp -f " + installDir + "/etc/Calpont.xml.rpmsave " + installDir + "/etc/Calpont.xml 2>&1";
		system(cmd.c_str());
	}

	string serverTypeInstall;
	int    IserverTypeInstall;
	try {
		serverTypeInstall = sysConfig->getConfig(InstallSection, "ServerTypeInstall");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting ServerTypeInstall from the Calpont System Configuration file" << endl;
		exit(1);
	}
	IserverTypeInstall = atoi(serverTypeInstall.c_str());

	if ( installType != "uninstall" ) {
		//setup ProcessConfig.xml
		switch ( IserverTypeInstall ) {
			case (oam::INSTALL_COMBINE_DM_UM_PM):	// combined #1 dm/um/pm on a single server
			case (oam::INSTALL_COMBINE_DM_UM):		// combined #2 dm/um on a same server
			case (oam::INSTALL_COMBINE_PM_UM):		// combined #3 um/pm on a same server
			{
				//module ProcessConfig.xml to setup all apps on the dm
				if( !updateProcessConfig(IserverTypeInstall) )
					cout << "Update ProcessConfig.xml error" << endl;
	
				break;
			}
		}
	
		//set Resource Settings
		switch ( IserverTypeInstall ) {
			case (oam::INSTALL_COMBINE_DM_UM_PM):	// combined #1 - dm/um/pm on a single server
			{	
				if ( !writeConfig(sysConfig) ) {
					cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
					exit(1);
				}
	
				try {
					sysConfig->setConfig("PrimitiveServers", "RotatingDestination", "n");
				}
				catch(...)
				{
					cout << "ERROR: Problem setting RotatingDestination in the Calpont System Configuration file" << endl;
					exit(1);
				}

				// are we using settings from previous config file?
				if ( reuseConfig == "n" ) {
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
				
					cout << "      Setting 'TotalUmMemory' to 25% of total memory. Value set to " << value << endl;
	
					try {
						sysConfig->setConfig("HashJoin", "TotalUmMemory", value);
					}
					catch(...)
					{
						cout << "ERROR: Problem setting TotalUmMemory in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
				}
				else
				{
					try {
						string numBlocksPct = sysConfig->getConfig("DBBC", "NumBlocksPct");
						string totalUmMemory = sysConfig->getConfig("HashJoin", "TotalUmMemory");

						if (numBlocksPct.empty() || numBlocksPct == "" ) {
							numBlocksPct = "50";

							try {
								sysConfig->setConfig("DBBC", "NumBlocksPct", "50");
		
								cout << endl << "NOTE: Setting 'NumBlocksPct' to 50%" << endl;
							}
							catch(...)
							{
								cout << "ERROR: Problem setting NumBlocksPct in the InfiniDB System Configuration file" << endl;
								exit(1);
							}
						}

						cout << endl << "NOTE: Using previous configuration setting for 'NumBlocksPct' = " << numBlocksPct << "%" << endl;
						cout << "      Using previous configuration setting for 'TotalUmMemory' = " << totalUmMemory << endl;
						
					}
					catch(...)
					{
						cout << "ERROR: Problem reading NumBlocksPct/TotalUmMemory in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
				}

				if ( !writeConfig(sysConfig) ) {
					cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
					exit(1);
				}
		
				break;
			}

			default:	// normal, seperate UM and PM
			{	
				// are we using settings from previous config file?
				if ( reuseConfig == "n" ) {
					cout << endl << "NOTE: Using the default setting for 'NumBlocksPct' at 70%" << endl;
	
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
					else if ( total <= 4000 )
						value = "1G";
					else if ( total <= 8000 )
						value = "2G";
					else if ( total <= 16000 )
						value = "4G";
					else if ( total <= 32000 )
						value = "8G";
					else if ( total <= 64000 )
						value = "16G";
					else
						value = "32G";
				
					cout << endl << "Setting 'TotalUmMemory' to 50% of total memory. Value set to " << value << endl;
	
					try {
						sysConfig->setConfig("HashJoin", "TotalUmMemory", value);
					}
					catch(...)
					{
						cout << "ERROR: Problem setting TotalUmMemory in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
				}
				else
				{
					try {
						string numBlocksPct = sysConfig->getConfig("DBBC", "NumBlocksPct");
						string totalUmMemory = sysConfig->getConfig("HashJoin", "TotalUmMemory");

						if (numBlocksPct.empty() || numBlocksPct == "" )
							numBlocksPct = "70";

						cout << endl << "NOTE: Using previous configuration setting for 'NumBlocksPct' = " << numBlocksPct << "%" << endl;
						cout << "      Using previous configuration setting for 'TotalUmMemory' = " << totalUmMemory << endl;
						
					}
					catch(...)
					{
						cout << "ERROR: Problem reading NumBlocksPct/TotalUmMemory in the InfiniDB System Configuration file" << endl;
						exit(1);
					}
				}

				if ( !writeConfig(sysConfig) ) {
					cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;
					exit(1);
				}
		
				break;
			}
		}
	}

	if ( !writeConfig(sysConfig) ) {                
		cout << "ERROR: Failed trying to update InfiniDB System Configuration file" << endl;                
		exit(1);        
	}

	//store syslog config file in Calpont Config file
	cmd = installDir + "/bin/syslogSetup.sh install  > /dev/null 2>&1";
	system(cmd.c_str());

	//get PPackage Type
	try{
		packageType = sysConfig->getConfig(InstallSection, "EEPackageType");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting EEPackageType" << endl;
		exit(1);
	}

	//get Parent OAM Module Name and setup of it's Custom OS files
	string parentOAMModuleName;
	try{
		parentOAMModuleName = sysConfig->getConfig(SystemSection, "ParentOAMModuleName");
		if ( parentOAMModuleName == oam::UnassignedName ) {
			cout << "ERROR: Parent OAM Module Name is unassigned" << endl;
			exit(1);
		}
	}
	catch(...)
	{
		cout << "ERROR: Problem getting Parent OAM Module Name" << endl;
		exit(1);
	}

	if ( installType == "initial" ) {
		//cleanup/create /local/etc directories
		cmd = "rm -rf " + installDir + "/local/etc > /dev/null 2>&1";
		system(cmd.c_str());
		cmd = "mkdir " + installDir + "/local/etc > /dev/null 2>&1";
		system(cmd.c_str());
	
		//create associated /local/etc directory for parentOAMModuleName
		cmd = "mkdir " + installDir + "/local/etc/" + parentOAMModuleName + " > /dev/null 2>&1";
		system(cmd.c_str());

		if (sysConfig->getConfig("Installation", "EnableSNMP") == "y")
		{
			//set SNMP Trap config file
			string NMSIPAddress;
			try {
				NMSIPAddress = sysConfig->getConfig(SystemSection, "NMSIPAddress");
		
				SNMPManager sm;
				sm.setNMSAddr(NMSIPAddress);
			}
			catch(...)
			{
				cout << "ERROR: Problem getting NMSIPAddress from Calpont System Configuration file" << endl;
				exit(1);
			}
		}
	}	
 
	//Get list of configured system modules
	SystemModuleTypeConfig sysModuleTypeConfig;

	try{
		oam.getSystemConfig(sysModuleTypeConfig);
	}
	catch(...)
	{
		cout << "ERROR: Problem reading the Calpont System Configuration file" << endl;
		exit(1);
	}

	string ModuleSection = "SystemModuleConfig";

	//get OAM Parent Module IP addresses and Host Name
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

	for ( unsigned int i = 0 ; i < sysModuleTypeConfig.moduletypeconfig.size(); i++)
	{
		string moduleType = sysModuleTypeConfig.moduletypeconfig[i].ModuleType;
		int moduleCount = sysModuleTypeConfig.moduletypeconfig[i].ModuleCount;

		if ( moduleCount == 0 )
			//no modules equipped for this Module Type, skip
			continue;

		//get IP addresses and Host Names
		DeviceNetworkList::iterator listPT = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
		for( ; listPT != sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; listPT++)
		{
			string moduleName = (*listPT).DeviceName;
			HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
			string moduleIPAddr = (*pt1).IPAddr;
			string moduleHostName = (*pt1).HostName;

			//save module info
			childmodule.moduleName = moduleName;
			childmodule.moduleIP = moduleIPAddr;
			childmodule.hostName = moduleHostName;

			if ( moduleName != parentOAMModuleName) {
				childmodulelist.push_back(childmodule);
			}

			if ( moduleType == "dm")
				directormodulelist.push_back(childmodule);
			if ( moduleType == "um")
				usermodulelist.push_back(childmodule);
			if ( moduleType == "pm")
				performancemodulelist.push_back(childmodule);

			if ( installType == "initial" ) {
				//create associated /local/etc directory for module
				cmd = "mkdir " + installDir + "/local/etc/" + moduleName + " > /dev/null 2>&1";
				system(cmd.c_str());
			}
		}
	} //end of i for loop

	if ( installType != "uninstall" ) {
		//setup rc.local file in local parent module
		if( !makeRClocal(parentOAMModuleName, IserverTypeInstall) )
			cout << "makeRClocal error" << endl;
	
		//create associated /local/etc directory for module
		// generate module
		ModuleList::iterator list1 = directormodulelist.begin();
		for (; list1 != directormodulelist.end() ; list1++)
		{
			cmd = "mkdir " + installDir + "/local/etc/" + (*list1).moduleName + " > /dev/null 2>&1";
			system(cmd.c_str());
	
			//make module file in /local/etc/"modulename"
			if( !makeModuleFile((*list1).moduleName, parentOAMModuleName) )
				cout << "makeModuleFile error" << endl;
	
			//setup rc.local file in module tmp dir
			if( !makeRClocal((*list1).moduleName, IserverTypeInstall) )
				cout << "makeRClocal error" << endl;
		}
	
		list1 = usermodulelist.begin();
		for (; list1 != usermodulelist.end() ; list1++)
		{
			cmd = "mkdir " + installDir + "/local/etc/" + (*list1).moduleName + " > /dev/null 2>&1";
			system(cmd.c_str());
	
			//make module file in /local/etc/"modulename"
			if( !makeModuleFile((*list1).moduleName, parentOAMModuleName) )
				cout << "makeModuleFile error" << endl;
	
			//setup rc.local file in module tmp dir
			if( !makeRClocal((*list1).moduleName, IserverTypeInstall) )
				cout << "makeRClocal error" << endl;
		}
	
		list1 = performancemodulelist.begin();
		for (; list1 != performancemodulelist.end() ; list1++)
		{
			cmd = "mkdir " + installDir + "/local/etc/" + (*list1).moduleName + " > /dev/null 2>&1";
			system(cmd.c_str());
	
			//make module file in /local/etc/"modulename"
			if( !makeModuleFile((*list1).moduleName, parentOAMModuleName) )
				cout << "makeModuleFile error" << endl;
	
			//setup rc.local file in module tmp dir
			if( !makeRClocal((*list1).moduleName, IserverTypeInstall) )
				cout << "makeRClocal error" << endl;
		}
	}

	//perform uninstall option
	if ( installType == "uninstall" ) {
		ModuleList::iterator list1 = childmodulelist.begin();
		for (; list1 != childmodulelist.end() ; list1++)
		{
			string remoteModuleName = (*list1).moduleName;
			string remoteModuleIP = (*list1).moduleIP;
			string remoteHostName = (*list1).hostName;
			string remoteModuleType = remoteModuleName.substr(0,MAX_MODULE_TYPE_SIZE);

			if ( remoteModuleType == "um" ||
				(remoteModuleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM) )
			{
				//run remote installer script
				if ( packageType != "binary" ) {
					string temppwprompt = pwprompt;
					if ( pwprompt == " " )
						temppwprompt = "none";
						
					cout << endl << "----- Performing Uninstall on Module '" + remoteModuleName + "' -----" << endl << endl;
					cmd = installDir + "/bin/user_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpont_rpm + " " + mysql_rpm + " " + mysqld_rpm + " " + installType + " " + packageType + " " + nodeps + " " + temppwprompt + " " + installer_debug;
					int rtnCode = system(cmd.c_str());
					if (rtnCode != 0) {
						cout << endl << "ERROR: returned from user_installer.sh" << endl;
						exit(1);
					}
				}
				else
				{	// do a binary package install
					cmd = installDir + "/bin/binary_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " +
						password + " " + calpont_rpm + " " + remoteModuleType + " " + installType + " " +
						serverTypeInstall + " " + installer_debug + " " + installDir;
					int rtnCode = system(cmd.c_str());
					if (rtnCode != 0) {
						cout << endl << "ERROR: returned from binary_installer.sh" << endl;
						exit(1);
					}
				}
			}
			else
			{
				if (remoteModuleType == "pm")
				{
					if ( packageType != "binary" ) {
						//run remote installer script
						cout << endl << "----- Performing Uninstall on Module '" + remoteModuleName + "' -----" << endl << endl;
						cmd = installDir + "/bin/performance_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpont_rpm + " " + mysql_rpm + " " + mysqld_rpm + " " + installType + " " + packageType + " " + nodeps + " " + installer_debug;
						int rtnCode = system(cmd.c_str());
						if (rtnCode != 0) {
							cout << endl << "ERROR returned from performance_installer.sh" << endl;
							exit(1);
						}
					}
					else
					{	// do a binary package install
						cmd = installDir + "/bin/binary_installer.sh " + remoteModuleName + " " + remoteModuleIP +
							" " + password + " " + calpont_rpm + " " + remoteModuleType + " " + installType + " " +
							serverTypeInstall + " " + installer_debug + " " + installDir;
						int rtnCode = system(cmd.c_str());
						if (rtnCode != 0) {
							cout << endl << "ERROR returned from binary_installer.sh" << endl;
							exit(1);
						}
					}
				}
			}

			//unmount child module
			cmd = "umount -fl /mnt/" + remoteModuleName + " > /dev/null 2>&1";
			system(cmd.c_str());
			cmd = "rm -fr /mnt/" + remoteModuleName;
			system(cmd.c_str());		
		}
		cout << "uninstall request successful" << endl;
		return 0;
	}

	if ( installType == "initial" ) {
		//setup local OS Files
		if( !setOSFiles(parentOAMModuleName, IserverTypeInstall) ) {
			cout << "ERROR: setOSFiles error" << endl;
			exit(1);
		}

		cmd = "chmod 1777 -R " + installDir + "/data1/systemFiles/dbrm > /dev/null 2>&1";
		system(cmd.c_str());

		SNMPManager sm;
		sm.updateSNMPD("parentOAMIPStub", parentOAMModuleIPAddr);
	}

	string idbstartcmd = installDir + "/bin/infinidb start";
	if ( IserverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM || 
			performancemodulelist.size() > 1 ) {
		// 
		// perform multi-server install
		//

		// perform remote install of servers in the system
		ModuleList::iterator list1 = childmodulelist.begin();
			for (; list1 != childmodulelist.end() ; list1++)
		{
			string remoteModuleName = (*list1).moduleName;
			string remoteModuleIP = (*list1).moduleIP;
			string remoteHostName = (*list1).hostName;
			string remoteModuleType = remoteModuleName.substr(0,MAX_MODULE_TYPE_SIZE);

			if ( remoteModuleType == "um" ||
				(remoteModuleType == "pm" && IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM) )
			{
				//run remote installer script
				if ( packageType != "binary" ) {
					string temppwprompt = pwprompt;
					if ( pwprompt == " " )
						temppwprompt = "none";
						
					cout << endl << "----- Performing Install on Module '" + remoteModuleName + "' -----" << endl << endl;
					cmd = installDir + "/bin/user_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpont_rpm + " " + mysql_rpm + " " + mysqld_rpm + " " + installType + " " + packageType + " " + nodeps + " " + temppwprompt + " " + installer_debug;
					int rtnCode = system(cmd.c_str());
					if (rtnCode != 0) {
						cout << endl << "ERROR returned from user_installer.sh" << endl;
						exit(1);
					}
				}
				else
				{	// do a binary package install
					cmd = installDir + "/bin/binary_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " +
						password + " " + calpont_rpm + " " + remoteModuleType + " " + installType + " " +
						serverTypeInstall + " " + installer_debug + " " + installDir;
					int rtnCode = system(cmd.c_str());
					if (rtnCode != 0) {
						cout << endl << "ERROR returned from binary_installer.sh" << endl;
						exit(1);
					}
				}
			}
			else
			{
				if (remoteModuleType == "pm" && IserverTypeInstall != oam::INSTALL_COMBINE_DM_UM_PM)
				{
					//run remote installer script
					if ( packageType != "binary" ) {
						cout << endl << "----- Performing Install on Module '" + remoteModuleName + "' -----" << endl << endl;
						cmd = installDir + "/bin/performance_installer.sh " + remoteModuleName + " " + remoteModuleIP + " " + password + " " + calpont_rpm + " " + mysql_rpm + " " + mysqld_rpm + " " + installType + " " + packageType + " " + nodeps + " " + installer_debug;
						int rtnCode = system(cmd.c_str());
						if (rtnCode != 0) {
							cout << endl << "ERROR returned from performance_installer.sh" << endl;
							exit(1);
						}
					}
					else
					{	// do a binary package install
						cmd = installDir + "/bin/binary_installer.sh " + remoteModuleName + " " + remoteModuleIP +
							" " + password + " " + calpont_rpm + " " + remoteModuleType + " " + installType +
							" " + serverTypeInstall + " " + installer_debug + " " + installDir;
						int rtnCode = system(cmd.c_str());
						if (rtnCode != 0) {
							cout << endl << "ERROR returned from binary_installer.sh" << endl;
							exit(1);
						}
					}
				}
			}
		}
	
		if ( IserverTypeInstall == oam::INSTALL_COMBINE_DM_UM_PM )
		{
			//run the mysql / mysqld setup scripts
			cout << endl << "Running the InfiniDB MySQL setup scripts" << endl << endl;
	
			// call the mysql setup scripts
			mysqlSetup();
			sleep(5);
		}

		cout << "System Install successfully completed, starting InfiniDB" << endl;

		//
		// perform start of InfiniDB of other servers in the system
		//
		list1 = childmodulelist.begin();
		
		for (; list1 != childmodulelist.end() ; list1++)
		{
			string remoteModuleName = (*list1).moduleName;
			string remoteModuleIP = (*list1).moduleIP;
			string remoteHostName = (*list1).hostName;
	
			if ( remoteModuleName == parentOAMModuleName )
				continue;
	
			//run remote command script
			cmd = installDir + "/bin/remote_command.sh " + remoteModuleIP + " " + password + " '" + idbstartcmd + "' " +  installer_debug;
			int rtnCode = system(cmd.c_str());
			if (rtnCode != 0)
				cout << "error with running remote_command.sh" << endl;
		}

		//start on local module
		int rtnCode = system(idbstartcmd.c_str());
		if (rtnCode != 0)
			cout << "Error starting InfiniDB local module" << endl;
		else
			cout << "Start InfiniDB request successful" << endl;
	}
	else
	{
		// 
		// perform single-server install from auto-installer
		//
		if ( calpont_rpm != "dummy.rpm" ) {

			//run the mysql / mysqld setup scripts
			cout << endl << "Running the InfiniDB MySQL setup scripts" << endl << endl;
	
			// call the mysql setup scripts
			mysqlSetup();
			sleep(5);
	
			//start on local module
			int rtnCode = system(idbstartcmd.c_str());
			if (rtnCode != 0)
				cout << "Error starting InfiniDB local module" << endl;
			else
				cout << "Start InfiniDB request successful" << endl;
		}
		else
		{
			// 
			// perform single-server install from postConfigure
			//

			//run the mysql / mysqld setup scripts
			cout << endl << "Running the Infinidb MySQL setup scripts" << endl << endl;
	
			// call the mysql setup scripts
			mysqlSetup();
			sleep(5);

			//startup mysqld and infinidb processes
			cout << endl;
			cmd = installDir + "/bin/clearShm > /dev/null 2>&1";
			system(cmd.c_str());
			system(idbstartcmd.c_str());
		}
	}

	// check for system going ACTIVE
	sleep(5);
	cout << endl << "InfiniDB Database Platform Starting, please wait ."; 
	cout.flush();

	if ( waitForActive() ) {
		cout << " DONE" << endl;

		cmd = installDir + "/bin/dbbuilder 7 > /tmp/dbbuilder.log";
		system(cmd.c_str());

		if (oam.checkLogStatus("/tmp/dbbuilder.log", "System Catalog created") )
			cout << endl << "System Catalog Successfull Created" << endl;
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

		cout << ". " + installDir + "/bin/calpontAlias" << endl << endl;

		cout << "Enter 'idbmysql' to access the InfiniDB MySQL console" << endl;
		cout << "Enter 'cc' to access the InfiniDB OAM console" << endl << endl;
	}
	else
	{
		cout << " FAILED" << endl;
		cout << endl << "ERROR: InfiniDB Process failed to start, check log files in /var/log/Calpont" << endl;
	}
}


/*
 * Setup OS Files by appending the Calpont versions
 */

// /etc OS Files to be updated
string files[] = {
	"rc.local",
	" "
};

bool setOSFiles(string parentOAMModuleName, int serverTypeInstall)
{
	string cmd;
	bool allfound = true;

	//update /etc files
	for ( int i=0;;++i)
	{
		if ( files[i] == " ")
			//end of list
			break;

		//create or update date on file to make sure on exist
		if ( files[i] == "rc.local") {
			cmd = "touch " + installDir + "/local/etc/" + parentOAMModuleName + "/rc.local.calpont"; 
			system(cmd.c_str());
		}

		string fileName = "/etc/" + files[i];

		//make a backup copy before changing
		cmd = "rm -f " + fileName + ".calpontSave";
		system(cmd.c_str());

		cmd = "cp " + fileName + " " + fileName + ".calpontSave > /dev/null 2>&1";
		system(cmd.c_str());

		cmd = "cat " + installDir + "/local/etc/" + parentOAMModuleName + "/" + files[i] + ".calpont >> " + fileName;
		if (geteuid() == 0) system(cmd.c_str());

		cmd = "rm -f " + installDir + "/local/ " + files[i] + "*.calpont > /dev/null 2>&1";
		system(cmd.c_str());

		cmd = "cp " + installDir + "/local/etc/" + parentOAMModuleName + "/" + files[i] + ".calpont " + installDir + "/local/. > /dev/null 2>&1"; 
		system(cmd.c_str());
	}

	return allfound;
}

/*
 * Updated snmpd.conf with parentOAMModuleIPAddr
 */
bool updateSNMPD(string parentOAMModuleIPAddr)
{
	string fileName = installDir + "/etc/snmpd.conf";

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return false;
	
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
 * Create a module file
 */
bool makeModuleFile(string moduleName, string parentOAMModuleName)
{
	string cmd;
	string fileName;
	if ( moduleName == parentOAMModuleName )
		fileName = installDir + "/local/module";
	else
		fileName = installDir + "/local/etc/" + moduleName + "/module";

	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());

	cmd = "echo " + moduleName + " > " + fileName;
	system(cmd.c_str());

	newFile.close();

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
	string cmd;

	switch ( serverTypeInstall ) {
		case (oam::INSTALL_COMBINE_DM_UM_PM):
		{
			newModule = ">pm";
			oldModule.push_back(">um");
			oldModule.push_back(">pm");
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

	string fileName = installDir + "/etc/ProcessConfig.xml";

	//Save a copy of the original version
	cmd = "/bin/cp -f " + fileName + " " + fileName + ".calpontSave > /dev/null 2>&1";
	system(cmd.c_str());

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return false;
	
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
 * Make makeRClocal to set mount scheduler
 */
bool makeRClocal(string moduleName, int IserverTypeInstall)
{
	string cmd;
	string fileName = installDir + "/local/etc/" + moduleName + "/rc.local.calpont";

	string moduleType = moduleName.substr(0,MAX_MODULE_TYPE_SIZE);

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
		string line2 = "        echo deadline > /sys/block/$scsi_dev/queue/scheduler";
		string line3 = "done";
	
		lines.push_back(line1);
		lines.push_back(line2);
		lines.push_back(line3);
	}
	else
	{
		if ( !mount2.empty() ) {
			string line1 = "for scsi_dev in `mount | awk '" + mount2 + " {print $1}' | awk -F/ '{print $3}' | sed 's/[0-9]*$//'`; do";
			string line2 = "        echo deadline > /sys/block/$scsi_dev/queue/scheduler";
			string line3 = "done";
		
			lines.push_back(line1);
			lines.push_back(line2);
			lines.push_back(line3);
		}
	}

	unlink (fileName.c_str());

	if ( lines.begin() == lines.end()) {
		cmd = "touch " + fileName + " >/dev/null 2>&1";
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
 * Uncomment entry in Calpont.xml
 */
bool uncommentCalpontXml( string entry)
{
	string fileName = installDir + "/etc/Calpont.xml";

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return true;
	
	vector <string> lines;
	char line[200];
	string buf;
	string newLine;

	string firstComment = "<!--";
	string lastComment = "-->";

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
// vim:ts=4 sw=4:

