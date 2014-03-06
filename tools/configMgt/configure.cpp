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

/******************************************************************************************
* $Id: configure.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
* List of files being updated by configure:
*		Calpont/etc/Calpont.xml
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
#include "stdio.h"
#include "ctype.h"
#include <netdb.h>

#include <readline/readline.h>
#include <readline/history.h>

#include "liboamcpp.h"
#include "configcpp.h"

using namespace std;
using namespace oam;
using namespace config;

typedef std::vector<string> Devices;

typedef struct Performance_Module_struct
{
	std::string     moduleIP1;
	std::string     moduleIP2;
	std::string     moduleIP3;
	std::string     moduleIP4;
} PerformanceModule;

typedef std::vector<PerformanceModule> PerformanceModuleList;

int main(int argc, char *argv[])
{

	setenv("CALPONT_HOME", ".", 1);

    char* pcommand = 0;
	string parentOAMModuleName;
	string parentOAMModuleIPAddr;
	PerformanceModuleList performancemodulelist;
	int pmNumber = 0;
	string prompt;
	int DBRMworkernodeID = 0;
	string parentOAMModuleHostName;
	string password;

	string SystemSection = "SystemConfig";
	string InstallSection = "Installation";

	cout << endl;
	cout << "This is the Calpont System Configuration tool." << endl;
	cout << "It will generate a Calpont System specific Calpont.xml file." << endl;
	cout << "The file can then be used by the autoInstaller tool" << endl;
	cout << endl;

	cout << "Instructions:" << endl << endl;
	cout << "	Press 'enter' to accept a default value in (), if available or" << endl;
	cout << "	Enter one of the options within [], if available, or" << endl;
	cout << "	Enter a new value" << endl << endl;

	//get system name
	string systemName;
	Config* netConfig = Config::makeConfig("./systems/CalpontSystems.xml");


	while(true)
	{
		prompt = "Enter Calpont system name > ";
		pcommand = readline(prompt.c_str());
		if (pcommand) {
			systemName = pcommand;
			free(pcommand);
			pcommand = 0;

			int systemCount;
			try {
				systemCount = strtol(netConfig->getConfig("NetworkConfig", "SystemCount").c_str(), 0, 0);
				if ( systemCount == 0 ) {
					cout << "ERROR: SystemCount in ./systems/CalpontSystems.xml equal to 0" << endl;
					exit(-1);
				}
			}
			catch(...)
			{
				cout << "ERROR: Problem getting SystemCount from ./systems/CalpontSystems.xml" << endl;
				exit(-1);
			}
		
			bool FOUND = false;
			for ( int i = 1 ; i < systemCount+1 ; i++)
			{
			    Oam oam;
				string SystemName = "SystemName" + oam.itoa(i);
				string oamParentModule = "OAMParentModule" + oam.itoa(i);
				string SystemPassword = "SystemPassword" + oam.itoa(i);
		
				string tempSystem;
				try {
					tempSystem = netConfig->getConfig("NetworkConfig", SystemName );
				}
				catch(...)
				{
					cout << "ERROR: Problem getting SystemName from ./systems/CalpontSystems.xml" << endl;
					exit(-1);
				}
		
				if ( tempSystem == systemName ) {
					try {
						parentOAMModuleHostName = netConfig->getConfig("NetworkConfig", oamParentModule );
						password = netConfig->getConfig("NetworkConfig", SystemPassword );

						string cmd = "mkdir systems/" + systemName + " > /dev/null 2>&1" ;
						system(cmd.c_str());

						FOUND = true;
						break;
					}
					catch(...)
					{
						cout << "ERROR: Problem getting SystemName from ./systems/CalpontSystems.xml" << endl;
						exit(-1);
					}
				}	
			}
		
			if ( !FOUND ) {
				cout << "ERROR: System Name '" + systemName + "' not in ./systems/CalpontSystems.xml" << endl;
				continue;
			}

			break;
		}
		cout << "Invalid System Name, please re-enter" << endl;
	}

	//determine which Calpont.xml to use as a base
	while(true)
	{
		cout << endl;
		cout << "Enter the Calpont.xml file do you want to use as a based version" << endl;
		cout << "Enter: 1 for System version (meaning copy from the system)" << endl; 
		cout << "       2 for Release version" << endl; 
		cout << "       3 for Calpont.xml version already located in the systems directory" << endl; 

		int option;
		prompt = "Enter (1,2,or 3) > ";
		pcommand = readline(prompt.c_str());
		if (!pcommand)
			continue;
		else
		{
			option = atoi(pcommand);
			free(pcommand);
			pcommand = 0;
			switch(option) {
				case 1:	//get system Calpont.xml
				{
					cout << "Copying from system, please wait...    " << flush;
					//get Network IP Address
				    Oam oam;
					parentOAMModuleIPAddr = oam.getIPAddress( parentOAMModuleHostName);
					if ( parentOAMModuleIPAddr.empty() ) {
						cout << "Invalid Host Name (No DNS IP found), exiting..." << endl;
						exit (-1);
					}
				
					//check to see if system is up
					string cmdLine = "ping ";
					string cmdOption = " -w 1 >> /dev/null";
				
					string cmd = cmdLine + parentOAMModuleIPAddr + cmdOption;
					int rtnCode = system(cmd.c_str());
					if ( rtnCode != 0 ) {
						cout << "System is down, exiting..." << endl;
						exit (-1);
					}

					cmd = "./remote_scp_get.sh " + parentOAMModuleIPAddr + " " + password + " /usr/local/Calpont/etc/Calpont.xml 0 ";
					rtnCode = system(cmd.c_str());
					if (rtnCode == 0) {
						cmd = "mv Calpont.xml systems/" + systemName + "/.";
						rtnCode = system(cmd.c_str());
						if ( rtnCode != 0 ) {
							cout << "ERROR: No system Calpont.xml found" << endl;
							continue;
						}
						else
							break;
					}
					break;
				}
				case 2:	//get release Calpont.xml
				{
					string release;
					while (true)
					{
						prompt = "Enter Calpont Release number > ";
						pcommand = readline(prompt.c_str());
						if (!pcommand)
							continue;
						else 
						{
							release = pcommand;
							free(pcommand);
							pcommand = 0;
							if (release.empty()) continue;
							string cmd = "cd systems/" + systemName + ";rm -f Calpont.xml;smbclient //cal6500/shared -Wcalpont -U" + oam::USERNAME + "%" + oam::PASSWORD + " -c 'cd Iterations/" + release + "/;prompt OFF;mget Calpont.xml' > /dev/null 2>&1";
							int rtnCode = system(cmd.c_str());
							if (rtnCode != 0)
								cout << "FAILED: no Release Calpont.xml found for " + release << endl;
							else
							{
								cmd = "cd systems/" + systemName + ";ls Calpont.xml > /dev/null 2>&1";
								rtnCode = system(cmd.c_str());
								if (rtnCode != 0) {
									cout << "FAILED: no Release Calpont.xml found for " + release  << endl;
									continue;
								}
								else
									break;
							}
						}
					}
					break;
				}
				case 3:	//use Calpont.xml alyread in system directory
					break;

				default:
					continue;
			}
			break;
		}
	}

	cout << endl;

	string env = "systems/" + systemName;

	setenv("CALPONT_HOME", env.c_str() , 1);
    Oam oam;

	Config* sysConfig = Config::makeConfig(env + "/Calpont.xml");

	// make DBRM backwards compatiable for pre 1.0.0.157 load
	string dbrmMainProc = "DBRM_Controller";
	string dbrmSubProc = "DBRM_Worker";
	string numSubProc = "NumWorkers";

	try {
		if ( (sysConfig->getConfig(dbrmMainProc, "IPAddr")).empty() ) {
			dbrmMainProc = "DBRM_Master";
			dbrmSubProc = "DBRM_Slave";
			numSubProc = "NumSlaves";
		}
	}
	catch(...)
	{
		dbrmMainProc = "DBRM_Master";
		dbrmSubProc = "DBRM_Slave";
		numSubProc = "NumSlaves";
	}

	string singleServerInstall;
	try {
		singleServerInstall = sysConfig->getConfig(InstallSection, "SingleServerInstall");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting SingleServerInstall from the Calpont System Configuration file" << endl;
		exit(-1);
	}

	//get Parent OAM Module Name and setup of it's Custom OS files
	try{
		parentOAMModuleName = sysConfig->getConfig(SystemSection, "ParentOAMModuleName");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting Parent OAM Module Name" << endl;
		exit(-1);
	}

	while(true)
	{
		prompt = "Single Server Installation? [y,n] (" + singleServerInstall + ") > ";
		pcommand = readline(prompt.c_str());
		cout << endl;
		if (pcommand)
		{
			if (strlen(pcommand) > 0) singleServerInstall = pcommand;
			free(pcommand);
			pcommand = 0;
		}

		if ( singleServerInstall == "y" ) {
			cout << "Single Server Installation will be performed. The Server will be assigned as a Director Module #1." << endl;
			cout << "All Calpont Processes will run on this single server." << endl;
		}
		else
		{
			if ( singleServerInstall == "n" ) {
				prompt = "Enter the OAM Parent Module Name or exit [mm1,dmx,exit] (" + parentOAMModuleName + ") > ";
				pcommand = readline(prompt.c_str());
				if (pcommand) {
					if (strlen(pcommand) > 0) parentOAMModuleName = pcommand;
					free(pcommand);
					pcommand = 0;
					if (parentOAMModuleName == "exit")
						exit(0);
				}
				break;
			}
			else
				cout << "Invalid Entry, please enter 'y' for yes or 'n' for no" << endl;
		}
		break;
	}

	// set Single Server Installation Indicator
	try {
		sysConfig->setConfig(InstallSection, "SingleServerInstall", singleServerInstall);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting DBRootStorageLoc in the Calpont System Configuration file" << endl;
		exit(-1);
	}

	//set Parent OAM Module Name
	try{
		sysConfig->setConfig(SystemSection, "ParentOAMModuleName", parentOAMModuleName);
	}
	catch(...)
	{
		cout << "ERROR: Problem updating the Calpont System Configuration file" << endl;
		exit(-1);
	}

	string parentOAMModuleType = parentOAMModuleName.substr(0,MAX_MODULE_TYPE_SIZE);
	int parentOAMModuleID = atoi(parentOAMModuleName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());

	//Get list of configured system modules
	SystemModuleTypeConfig sysModuleTypeConfig;

	try{
		oam.getSystemConfig(sysModuleTypeConfig);
	}
	catch(...)
	{
		cout << "ERROR: Problem reading the Calpont System Configuration file" << endl;
		exit(-1);
	}

	//
	// get Data storage Mount
	//
	string DBRootStorageLoc;
	string DBRootStorageType;
	string UserStorageType;
	string UserStorageLoc;

	string OAMStorageType;
	string OAMStorageLoc;

	int DBRootCount;
	string deviceName;
	Devices devices;

	cout << endl << "===== Setup Data Storages Mount Configuration =====" << endl << endl;

	try {
		DBRootStorageType = sysConfig->getConfig(InstallSection, "DBRootStorageType");
		DBRootCount = strtol(sysConfig->getConfig(SystemSection, "DBRootCount").c_str(), 0, 0);
		OAMStorageType = sysConfig->getConfig(InstallSection, "OAMStorageType");
		OAMStorageLoc = sysConfig->getConfig(InstallSection, "OAMStorageLoc");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting DB Storage Data from the Calpont System Configuration file" << endl;
		exit(-1);
	}

	//setup dbroot storage
	while(true)
	{
		prompt = "Enter Calpont Data Storage Mount Type [storage,local,nfs] (" + DBRootStorageType + ") > ";
		pcommand = readline(prompt.c_str());
		if (pcommand)
		{
			if (strlen(pcommand) > 0) DBRootStorageType = pcommand;
			free(pcommand);
			pcomand = 0;
		}
		if ( DBRootStorageType == "nfs" || DBRootStorageType == "storage" || DBRootStorageType == "local")
			break;
		cout << "Invalid Mount Type, please re-enter" << endl;
	}

	try {
		sysConfig->setConfig(InstallSection, "DBRootStorageType", DBRootStorageType);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting DBRootStorageType in the Calpont System Configuration file" << endl;
		exit(-1);
	}

	while(true)
	{
		prompt = "Enter the Number of Calpont Data Storage (DBRoots) areas (" + oam.itoa(DBRootCount) + ") > ";
		pcommand = readline(prompt.c_str());
		if (pcommand) {
			int newDBRootCount = DBRootCount;
			if (strlen(pcommand) > 0) newDBRootCount = atoi(pcommand);
			free(pcommand);
			pcommand = 0;
			if (newDBRootCount <= 0) {
				cout << "ERROR: Invalid Number, please reenter" << endl;
				continue;
			}

			DBRootCount = newDBRootCount;
			try {
				sysConfig->setConfig(SystemSection, "DBRootCount", oam.itoa(DBRootCount));
			}
			catch(...)
			{
				cout << "ERROR: Problem setting DBRoot Count in the Calpont System Configuration file" << endl;
				exit(-1);
			}
		}
		break;
	}

	for( int i=1 ; i < DBRootCount+1 ; i++)
	{
		if ( DBRootStorageType != "local") {

			string DBRootStorageLocID = "DBRootStorageLoc" + oam.itoa(i);
			try {
				DBRootStorageLoc = sysConfig->getConfig(InstallSection, DBRootStorageLocID);
			}
			catch(...)
			{
				cout << "ERROR: Problem getting DB Storage Data from the Calpont System Configuration file" << endl;
				exit(-1);
			}
	
			prompt = "Enter Storage Location for DBRoot #" + oam.itoa(i) + " (" + DBRootStorageLoc + ") > ";
			pcommand = readline(prompt.c_str());
			if (pcommand)
			{
				if (strlen(pcommand) > 0) DBRootStorageLoc = pcommand;
				free(pcommand);
				pcommand = 0;
			}
	
			devices.push_back(DBRootStorageLoc);
	
			try {
				sysConfig->setConfig(InstallSection, DBRootStorageLocID, DBRootStorageLoc);
			}
			catch(...)
			{
				cout << "ERROR: Problem setting DBRootStorageLoc in the Calpont System Configuration file" << endl;
				exit(-1);
			}
		}

		string DBrootID = "DBRoot" + oam.itoa(i);
		string pathID =  "/usr/local/Calpont/data" + oam.itoa(i);

		try {
			sysConfig->setConfig(SystemSection, DBrootID, pathID);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting DBRoot in the Calpont System Configuration file" << endl;
			exit(-1);
		}
	}

	//setup OAM storage
	cout << endl;
	while(true)
	{
		prompt = "Enter OAM Data Storage Mount Type [storage,local] (" + OAMStorageType + ") > ";
		pcommand = readline(prompt.c_str());
		if (pcommand)
		{
			if (strlen(pcommand) > 0) OAMStorageType = pcommand;
			free(pcommand);
			pcommand = 0;
		}
		if ( OAMStorageType == "storage" || OAMStorageType == "local" )
			break;
		cout << "Invalid Mount Location, please re-enter" << endl;
	}

	try {
		sysConfig->setConfig(InstallSection, "OAMStorageType", OAMStorageType);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting OAMStorageType in the Calpont System Configuration file" << endl;
		exit(-1);
	}

	if ( OAMStorageType == "storage") {
		cout << endl;
		prompt = "Enter Device Name for OAM Storage mounting (" + OAMStorageLoc + ") > ";
		pcommand = readline(prompt.c_str());
		if (pcommand)
		{
			if (strlen(pcommand) > 0) OAMStorageLoc = pcommand;
			free(pcommand);
			pcommand = 0;
		}

		try {
			sysConfig->setConfig(InstallSection, "OAMStorageLoc", OAMStorageLoc);
		}
		catch(...)
		{
			cout << "ERROR: Problem setting OAMStorageLoc in the Calpont System Configuration file" << endl;
			exit(-1);
		}
	}

	sysConfig->write();

	//
	// Module Configuration
	//
	cout << endl << "===== Setup the Module Configuration =====" << endl;

	string ModuleSection = "SystemModuleConfig";
	unsigned int maxPMNicCount = 1;

	for ( unsigned int i = 0 ; i < sysModuleTypeConfig.moduletypeconfig.size(); i++)
	{
		string moduleType = sysModuleTypeConfig.moduletypeconfig[i].ModuleType;
		string moduleDesc = sysModuleTypeConfig.moduletypeconfig[i].ModuleDesc;
		int moduleCount = sysModuleTypeConfig.moduletypeconfig[i].ModuleCount;

		//verify and setup of modules count

		if ( moduleType == "dm" && singleServerInstall == "y" ) {

			cout << endl << "----- " << moduleDesc << " Configuration -----" << endl << endl;

			moduleCount = 1;
			pmNumber = 1;
			try {
				string ModuleCountParm = "ModuleCount" + oam.itoa(i+1);
				sysConfig->setConfig(ModuleSection, ModuleCountParm, oam.itoa(moduleCount));
			}
			catch(...)
			{
				cout << "ERROR: Problem setting Module Count in the Calpont System Configuration file" << endl;
				exit(-1);
			}
		}
		else
		{
			if ( singleServerInstall == "y" ) {
				moduleCount = 0;
				try {
					string ModuleCountParm = "ModuleCount" + oam.itoa(i+1);
					sysConfig->setConfig(ModuleSection, ModuleCountParm, oam.itoa(moduleCount));
					continue;
				}
				catch(...)
				{
					cout << "ERROR: Problem setting Module Count in the Calpont System Configuration file" << endl;
					exit(-1);
				}
			}

			cout << endl << "----- " << moduleDesc << " Configuration -----" << endl << endl;

			while(true)
			{
				prompt = "Enter number of " + moduleDesc + "s (" + oam.itoa(moduleCount) + ") > ";
				pcommand = readline(prompt.c_str());
				if (pcommand) {
					//Update Count
					string ModuleCountParm = "ModuleCount" + oam.itoa(i+1);
					int newmoduleCount = moduleCount;
					if (strlen(pcommand) > 0) newmoduleCount = atoi(pcommand);
					free(pcommand);
					pcommand = 0;
					if (newmoduleCount <= 0) {
						cout << "ERROR: Invalid Number, please reenter" << endl;
						continue;
					}

					try {
						moduleCount = newmoduleCount;
						sysConfig->setConfig(ModuleSection, ModuleCountParm, oam.itoa(moduleCount));
					}
					catch(...)
					{
						cout << "ERROR: Problem setting Module Count in the Calpont System Configuration file" << endl;
						exit(-1);
					}
				}
	
				if ( parentOAMModuleType == moduleType && parentOAMModuleID > moduleCount ) {
					cout << endl << "ERROR: Parent OAM Module is '" + parentOAMModuleName + "', so you have to have at least 1 of this Module Type" << endl << endl;
				}
				else
				{
					break;
				}
			}
		}

		int listSize = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.size();

		if ( moduleCount == 0 ) {
			//set unEquipped Module IP addresses to oam::UnassignedIpAddr
			for ( int j = moduleCount ; j < listSize ; j ++ )
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
		if ( moduleType == "um" || singleServerInstall == "y" ) {
			try {
				UserStorageType = sysConfig->getConfig(InstallSection, "UserStorageType");
			}
			catch(...)
			{
				cout << "ERROR: Problem getting DB Storage Data from the Calpont System Configuration file" << endl;
				exit(-1);
			}

			if ( moduleType == "um" )
				cout << endl;

			//setup User Temp Storage
			while(true)
			{
				string newUserStorageType = UserStorageType;
				prompt = "Enter User Module Temp Data Storage Mount Type [storage,local] (" + UserStorageType + ") > ";
				pcommand = readline(prompt.c_str());
				if (pcommand)
				{
					if (strlen(pcommand) > 0) newUserStorageType = pcommand;
					free(pcommand);
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
				cout << "ERROR: Problem setting UserStorageType in the Calpont System Configuration file" << endl;
				exit(-1);
			}
		}

		int moduleID = 1;
		while(true) {
			prompt = "Enter Starting Module ID for " + moduleDesc + " (1) > ";
			pcommand = readline(prompt.c_str());
			if (pcommand)
			{
				if (strlen(pcommand) > 0) moduleID = atoi(pcommand);
				free(pcommand);
				pcommand = 0;
			}
	
			//valid if parent OAM module type and is consistent with parentOAMModuleName
			if ( parentOAMModuleType == moduleType && 
					( parentOAMModuleID < moduleID || parentOAMModuleID > moduleID + (moduleCount-1) ) ) {
				cout << endl << "ERROR: Parent and Starting Module ID out of range, please re-enter" << endl << endl;
				moduleID = 1;
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

		//get IP addresses and Host Names
		for( int k=0 ; k < moduleCount ; k++, moduleID++ )
		{
			PerformanceModule performancemodule;
			string newModuleName = moduleType + oam.itoa(moduleID);
			string moduleNameDesc = moduleDesc + " #" + oam.itoa(moduleID);

			cout << endl << "*** " << moduleNameDesc << " Configuration ***" << endl << endl;

			//Enter User Temp Storage location
			if ( moduleType == "um" || singleServerInstall == "y" ) {
				UserStorageLoc = oam::UnassignedName;
				string USERSTORAGELOC = "UserStorageLoc" + oam.itoa(moduleID);
				if ( UserStorageType == "storage") {
					try {
						UserStorageLoc = sysConfig->getConfig(InstallSection, USERSTORAGELOC);
					}
					catch(...)
					{
						cout << "ERROR: Problem getting DB Storage Data from the Calpont System Configuration file" << endl;
						exit(-1);
					}
		
					prompt = "Enter Device Name for User Module '" + newModuleName + "' Temp Storage (" + UserStorageLoc + ") > ";
					pcommand = readline(prompt.c_str());
					if (pcommand)
					{
						if (strlen(pcommand) > 0) UserStorageLoc = pcommand;
						free(pcommand);
						pcommand = 0;
					}
	
					try {
						sysConfig->setConfig(InstallSection, USERSTORAGELOC, UserStorageLoc);
					}
					catch(...)
					{
						cout << "ERROR: Problem setting UserStorageLoc in the Calpont System Configuration file" << endl;
						exit(-1);
					}
				}
				else
				{
					try {
						sysConfig->setConfig(InstallSection, USERSTORAGELOC, oam::UnassignedName);
					}
					catch(...)
					{
						cout << "ERROR: Problem setting UserStorageLoc in the Calpont System Configuration file" << endl;
						exit(-1);
					}
				}
			}

			//setup HostName/IPAddress for each NIC
			for( unsigned int nicID=1 ; nicID < MAX_NIC+1 ; nicID++ )
			{
				string moduleHostName = oam::UnassignedName;
				string moduleIPAddr = oam::UnassignedIpAddr;
	
				DeviceNetworkList::iterator listPT = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
				for( ; listPT != sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; listPT++)
				{
					HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
					for( ; pt1 != (*listPT).hostConfigList.end() ; pt1++)
					{
						if ( newModuleName == (*listPT).DeviceName && (*pt1).NicID == nicID) {
							moduleHostName = (*pt1).HostName;
							moduleIPAddr = (*pt1).IPAddr;
							break;
						}
					}
				}

				bool moduleHostNameFound = true;
				if (moduleHostName.empty()) {
					moduleHostNameFound = false;
					moduleHostName = oam::UnassignedName;
				}

				if (moduleIPAddr.empty())
					moduleIPAddr = oam::UnassignedIpAddr;
	
				string newModuleIPAddr;
				string newModuleHostName;
	
				while (true)
				{
					prompt = "Enter Nic Interface #" + oam.itoa(nicID) + " Host Name (" + moduleHostName + ") > ";
					pcommand = readline(prompt.c_str());
					if (!pcommand)
						newModuleHostName = moduleHostName;
					else
					{
						if (strlen(pcommand) > 0) newModuleHostName = pcommand;	
						free(pcommand);
						pcommand = 0;
					}
	
					if ( newModuleHostName == oam::UnassignedName && !moduleHostNameFound && nicID == 1 )
						cout << "Invalid Entry, please enter at least 1 valid Host Name for Module" << endl;
					else
						break;
				}

				if ( newModuleHostName == oam::UnassignedName && moduleHostNameFound )
					// exit out to next module ID
					break;
	
				//set New Module Host Name
				string moduleHostNameParm = "ModuleHostName" + oam.itoa(moduleID) + "-" + oam.itoa(nicID) + "-" + oam.itoa(i+1);
				try {
					sysConfig->setConfig(ModuleSection, moduleHostNameParm, newModuleHostName);
				}
				catch(...)
				{
					cout << "ERROR: Problem setting Host Name in the Calpont System Configuration file" << endl;
					exit(-1);
				}

				if ( newModuleHostName == oam::UnassignedName )
					newModuleIPAddr = oam::UnassignedIpAddr;
				else
				{
					//get Network IP Address
					string IPAddress = oam.getIPAddress( newModuleHostName);
					if ( !IPAddress.empty() )
						newModuleIPAddr = IPAddress;
					else
						newModuleIPAddr = moduleIPAddr;
	
					//prompt for IP address
					while (true)
					{
						prompt = "Enter Nic Interface #" + oam.itoa(nicID) + " IP Address of " + newModuleHostName + " (" + newModuleIPAddr + ") > ";
						pcommand = readline(prompt.c_str());
						if (pcommand)
						{
							if (strlen(pcommand) > 0) newModuleIPAddr = pcommand;	
							free(pcommand);
							pcommand = 0;
						}
						if (oam.isValidIP(newModuleIPAddr))
							break;
						else
							cout << "Invalid IP Address format, xxx.xxx.xxx.xxx, please reenter" << endl;
					}
				}

				//set Module IP address
				string moduleIPAddrNameParm = "ModuleIPAddr" + oam.itoa(moduleID) + "-" + oam.itoa(nicID) + "-" + oam.itoa(i+1);
				try {
					sysConfig->setConfig(ModuleSection, moduleIPAddrNameParm, newModuleIPAddr);
				}
				catch(...)
				{
					cout << "ERROR: Problem setting IP address in the Calpont System Configuration file" << endl;
					exit(-1);
				}

				if (moduleType == "pm" || ( moduleType == "dm" && singleServerInstall == "y" )) {
	
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

				//set port addresses
				if ( newModuleName == parentOAMModuleName ) {
					parentOAMModuleIPAddr = newModuleIPAddr;
	
					//set Parent Processes Port IP Address
					string parentProcessMonitor = parentOAMModuleName + "_ProcessMonitor";
					sysConfig->setConfig(parentProcessMonitor, "IPAddr", parentOAMModuleIPAddr);
					sysConfig->setConfig(parentProcessMonitor, "Port", "8606");
					sysConfig->setConfig("ProcMgr", "IPAddr", parentOAMModuleIPAddr);
					sysConfig->setConfig("ProcHeartbeatControl", "IPAddr", parentOAMModuleIPAddr);
					sysConfig->setConfig("ProcStatusControl", "IPAddr", parentOAMModuleIPAddr);
					string parentServerMonitor = parentOAMModuleName + "_ServerMonitor";
					sysConfig->setConfig(parentServerMonitor, "IPAddr", parentOAMModuleIPAddr);

					if ( singleServerInstall == "y" ) {
						//set User Module's IP Addresses
						string Section = "ExeMgr" + oam.itoa(moduleID);
		
						sysConfig->setConfig(Section, "IPAddr", newModuleIPAddr);
						sysConfig->setConfig(Section, "Port", "8601");
		
						//set Performance Module's IP's to first NIC IP entered
						sysConfig->setConfig("DDLProc", "IPAddr", newModuleIPAddr);
						sysConfig->setConfig("DMLProc", "IPAddr", newModuleIPAddr);
					}
				}
				else
				{
					//set child Process Monitor Port IP Address
					string portName = newModuleName + "_ProcessMonitor";
					sysConfig->setConfig(portName, "IPAddr", newModuleIPAddr);
					sysConfig->setConfig(portName, "Port", "8606");
	
					//set child Server Monitor Port IP Address
					portName = newModuleName + "_ServerMonitor";
					sysConfig->setConfig(portName, "IPAddr", newModuleIPAddr);
					sysConfig->setConfig(portName, "Port", "8622");
	
					//set User Module's IP Addresses
					if ( moduleType == "um" ) {
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
				
				//setup DBRM processes
				if ( newModuleName == "dm1" )
					sysConfig->setConfig(dbrmMainProc, "IPAddr", newModuleIPAddr);

				DBRMworkernodeID++;
				string DBRMSection = dbrmSubProc + oam.itoa(DBRMworkernodeID);
				sysConfig->setConfig(DBRMSection, "IPAddr", newModuleIPAddr);
				sysConfig->setConfig(DBRMSection, "Module", newModuleName);

			} //end of nicID loop

			if (moduleType == "pm" || singleServerInstall == "y" )
				performancemodulelist.push_back(performancemodule);

		} //end of k (moduleCount) loop

		sysConfig->write();

	} //end of i for loop

	//setup DBRM Controller
	sysConfig->setConfig(dbrmMainProc, numSubProc, oam.itoa(DBRMworkernodeID));

	//set ConnectionsPerPrimProc
	try {
		sysConfig->setConfig("PrimitiveServers", "ConnectionsPerPrimProc", oam.itoa(maxPMNicCount*2));
	}
	catch(...)
	{
		cout << "ERROR: Problem setting ConnectionsPerPrimProc in the Calpont System Configuration file" << endl;
		exit(-1);
	}

	//set the PM Ports based on Number of PM modules equipped, if any equipped
	int pmPorts = 32;
	sysConfig->setConfig("PrimitiveServers", "Count", oam.itoa(pmNumber));

	if ( pmNumber > 0 || singleServerInstall == "y" )
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

	sysConfig->write();

	//
	// Configure switches
	//
	SystemSwitchTypeConfig sysSwitchTypeConfig;

	try{
		oam.getSystemConfig(sysSwitchTypeConfig);
	}
	catch(...)
	{
		cout << "ERROR: Problem reading the Calpont System Configuration file" << endl;
		exit(-1);
	}

	cout << endl << "===== Setup the Switch Configuration =====" << endl;

	string SwitchSection = "SystemSwitchConfig";

	for ( unsigned int i = 0 ; i < sysSwitchTypeConfig.switchtypeconfig.size(); i++)
	{
		string switchType = sysSwitchTypeConfig.switchtypeconfig[i].SwitchType;
		string switchDesc = sysSwitchTypeConfig.switchtypeconfig[i].SwitchDesc;
		int switchCount = sysSwitchTypeConfig.switchtypeconfig[i].SwitchCount;

		//verify and setup of switches count
		cout << endl << "----- " << switchDesc << " Configuration -----" << endl << endl;

		while(true)
		{
			prompt = "Enter number of " + switchDesc + "es (" + oam.itoa(switchCount) + ") > ";
			pcommand = readline(prompt.c_str());
			if (pcommand) {
				//Update Count and continue with IP addresses
				string SwitchCountParm = "SwitchCount" + oam.itoa(i+1);
				int newswitchCount = switchCount;
				if (strlen(pcommand) > 0) newswitchCount = atoi(pcommand);
				free(pcommand);
				pcommand = 0;
				if (newswitchCount <= 0) {
					cout << "ERROR: Invalid Number, please reenter" << endl;
					continue;
				}

				try {
					switchCount = newswitchCount;
					sysConfig->setConfig(SwitchSection, SwitchCountParm, oam.itoa(switchCount));
					break;
				}
				catch(...)
				{
					cout << "ERROR: Problem setting Switch Count in the Calpont System Configuration file" << endl;
					exit(-1);
				}
			}
			break;
		}

		int listSize = sysSwitchTypeConfig.switchtypeconfig[i].SwitchNetworkList.size();

		//set unEquipped Switch IP addresses to oam::UnassignedIpAddr
		if ( switchCount < listSize )
		{
			for ( int j = switchCount ; j < listSize ; j ++ )
			{
				string SwitchIPAddr = "SwitchIPAddr" + oam.itoa(j+1) + "-" + oam.itoa(i+1);
				string SwitchHostName = "SwitchHostName" + oam.itoa(j+1) + "-" + oam.itoa(i+1);

				sysConfig->setConfig(SwitchSection, SwitchIPAddr, oam::UnassignedIpAddr);
				sysConfig->setConfig(SwitchSection, SwitchHostName, oam::UnassignedName);
			}
		}	

		if ( switchCount == 0 )
			//no switches equipped for this Switch Type, skip
			continue;

		//add new entried to Calpont System Config table for additional IP Addresses and Hostnames
		for ( int j = listSize ; j < switchCount ; j ++ )
		{
			string SwitchIPAddr = "SwitchIPAddr" + oam.itoa(j+1) + "-" + oam.itoa(i+1);
			string SwitchHostName = "SwitchHostName" + oam.itoa(j+1) + "-" + oam.itoa(i+1);

			sysConfig->setConfig(SwitchSection, SwitchIPAddr, oam::UnassignedIpAddr);
			sysConfig->setConfig(SwitchSection, SwitchHostName, oam::UnassignedName);
		}

		//get IP addresses and Host Names
		for( int k=0 ; k < switchCount ; k++ )
		{
			int switchID = k+1;
			string newSwitchName = switchType + oam.itoa(switchID);
			string switchIPAddr = oam::UnassignedIpAddr;
			string switchHostName = oam::UnassignedName;
			string switchNameDesc = switchDesc + " #" + oam.itoa(switchID);

			cout << endl << "*** " << switchNameDesc << " Configuration ***" << endl << endl;

			DeviceNetworkList::iterator listPT = sysSwitchTypeConfig.switchtypeconfig[i].SwitchNetworkList.begin();
			for( ; listPT != sysSwitchTypeConfig.switchtypeconfig[i].SwitchNetworkList.end() ; listPT++)
			{
				if ( newSwitchName == (*listPT).DeviceName) {
					HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
					for( ; pt1 != (*listPT).hostConfigList.end() ; pt1++)
					{
						switchIPAddr = (*pt1).IPAddr;
						switchHostName = (*pt1).HostName;
						break;
					}
					break;
				}
			}

			if (switchIPAddr.empty())
				switchIPAddr = oam::UnassignedIpAddr;

			if (switchHostName.empty())
				switchHostName = oam::UnassignedName;

			string newSwitchIPAddr;
			string newSwitchHostName;

			prompt = "Enter Host Name (" + switchHostName + ") > ";
			pcommand = readline(prompt.c_str());
			if (!pcommand)
				newSwitchHostName = switchHostName;
			else
			{
				if (strlen(pcommand) > 0 )newSwitchHostName = pcommand;	
				free(pcommand);
				pcommand = 0;

				//set New Switch Host Name
				string switchHostNameParm = "SwitchHostName" + oam.itoa(switchID) + "-" + oam.itoa(i+1);
				try {
					sysConfig->setConfig(SwitchSection, switchHostNameParm, newSwitchHostName);
				}
				catch(...)
				{
					cout << "ERROR: Problem setting Host Name in the Calpont System Configuration file" << endl;
					exit(-1);
				}
			}

			//get Network IP Address
			string IPAddress = oam.getIPAddress( newSwitchHostName);
			if ( !IPAddress.empty() ) {
				newSwitchIPAddr = IPAddress;

				cout << "'" << newSwitchHostName << "' DNS IP Address is '" << newSwitchIPAddr << "'" << endl;
			}
			else
			{ //no DNS address found for Host Name
				while (true)
				{
					prompt = "Enter IP Address of (" + switchIPAddr + ") > ";
					pcommand = readline(prompt.c_str());
					if (!pcommand)
						newSwitchIPAddr = switchIPAddr;
					else
					{
						if (strlen(pcommand) > 0) newSwitchIPAddr = pcommand;	
						free(pcommand);
						pcommand = 0;
					}
					if (oam.isValidIP(newSwitchIPAddr))
						break;
					else
						cout << "Invalid IP Address format, xxx.xxx.xxx.xxx, please reenter" << endl;
				}
			}

			//set New Switch IP address
			string switchIPAddrNameParm = "SwitchIPAddr" + oam.itoa(switchID) + "-" + oam.itoa(i+1);
			try {
				sysConfig->setConfig(SwitchSection, switchIPAddrNameParm, newSwitchIPAddr);
			}
			catch(...)
			{
				cout << "ERROR: Problem setting IP address in the Calpont System Configuration file" << endl;
				exit(-1);
			}
		} //end of k loop
	} //end of i for loop

	sysConfig->write();

	//
	// Configure storages
	//
	SystemStorageTypeConfig sysStorageTypeConfig;

	try{
		oam.getSystemConfig(sysStorageTypeConfig);
	}
	catch(...)
	{
		cout << "ERROR: Problem reading the Calpont System Configuration file" << endl;
		exit(-1);
	}

	string StorageSection = "SystemStorageConfig";

	cout << endl << "===== Setup the Storage Configuration =====" << endl;

	for ( unsigned int i = 0 ; i < sysStorageTypeConfig.storagetypeconfig.size(); i++)
	{
		string storageType = sysStorageTypeConfig.storagetypeconfig[i].StorageType;
		string storageDesc = sysStorageTypeConfig.storagetypeconfig[i].StorageDesc;
		int storageCount = sysStorageTypeConfig.storagetypeconfig[i].StorageCount;

		//verify and setup of storages count
		cout << endl << "----- " << storageDesc << " Configuration -----" << endl << endl;

		while(true)
		{
			prompt = "Enter number of " + storageDesc + "s (" + oam.itoa(storageCount) + ") > ";
			pcommand = readline(prompt.c_str());
			if (pcommand) {
				//Update Count and continue with IP addresses
				string StorageCountParm = "StorageCount" + oam.itoa(i+1);
				int newstorageCount = storageCount;
				if (strlen(pcommand) > 0) newstorageCount = atoi(pcommand);
				free(pcommand);
				pcommand = 0;
				if (newstorageCount <= 0) {
					cout << "ERROR: Invalid Number, please reenter" << endl;
					continue;
				}

				try {
					storageCount = newstorageCount;
					sysConfig->setConfig(StorageSection, StorageCountParm, oam.itoa(storageCount));
					break;
				}
				catch(...)
				{
					cout << "ERROR: Problem setting Storage Count in the Calpont System Configuration file" << endl;
					exit(-1);
				}
			}
			break;
		}

		int listSize = sysStorageTypeConfig.storagetypeconfig[i].StorageNetworkList.size();

		//set unEquipped Storage IP addresses to oam::UnassignedIpAddr
		if ( storageCount < listSize )
		{
			for ( int j = storageCount ; j < listSize ; j ++ )
			{
				string storageIPAddr = "StorageIPAddr" + oam.itoa(j+1) + "-" + oam.itoa(i+1);
				string StorageHostName = "StorageHostName" + oam.itoa(j+1) + "-" + oam.itoa(i+1);

				sysConfig->setConfig(StorageSection, storageIPAddr, oam::UnassignedIpAddr);
				sysConfig->setConfig(StorageSection, StorageHostName, oam::UnassignedName);
			}
		}	

		if ( storageCount == 0 )
			//no storages equipped for this Storage Type, skip
			continue;

		//add new entried to Calpont System Config table for additional IP Addresses and Hostnames
		for ( int j = listSize ; j < storageCount ; j ++ )
		{
			string storageIPAddr = "StorageIPAddr" + oam.itoa(j+1) + "-" + oam.itoa(i+1);
			string StorageHostName = "StorageHostName" + oam.itoa(j+1) + "-" + oam.itoa(i+1);

			sysConfig->setConfig(StorageSection, storageIPAddr, oam::UnassignedIpAddr);
			sysConfig->setConfig(StorageSection, StorageHostName, oam::UnassignedName);
		}


		//get IP addresses and Host Names
		for( int k=0 ; k < storageCount ; k++ )
		{
			int storageID = k+1;
			string newStorageName = storageType + oam.itoa(storageID);
			string storageIPAddr = oam::UnassignedIpAddr;
			string storageHostName = oam::UnassignedName;
			string storageNameDesc = storageDesc + " #" + oam.itoa(storageID);

			cout << endl << "*** " << storageNameDesc << " Configuration ***" << endl << endl;

			DeviceNetworkList::iterator listPT = sysStorageTypeConfig.storagetypeconfig[i].StorageNetworkList.begin();
			for( ; listPT != sysStorageTypeConfig.storagetypeconfig[i].StorageNetworkList.end() ; listPT++)
			{
				if ( newStorageName == (*listPT).DeviceName) {
					HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
					for( ; pt1 != (*listPT).hostConfigList.end() ; pt1++)
					{
						storageIPAddr = (*pt1).IPAddr;
						storageHostName = (*pt1).HostName;
						break;
					}
					break;
				}
			}

			if (storageIPAddr.empty())
				storageIPAddr = oam::UnassignedIpAddr;

			if (storageHostName.empty())
				storageHostName = oam::UnassignedName;

			string newStorageIPAddr;
			string newStorageHostName;

			prompt = "Enter Host Name (" + storageHostName + ") > ";
			pcommand = readline(prompt.c_str());
			if (!pcommand)
				newStorageHostName = storageHostName;
			else
			{
				if (strlen(pcommand) > 0) newStorageHostName = pcommand;	
				free(pcommand);
				pcommand = 0;

				//set New Storage Host Name
				string storageHostNameParm = "StorageHostName" + oam.itoa(storageID) + "-" + oam.itoa(i+1);
				try {
					sysConfig->setConfig(StorageSection, storageHostNameParm, newStorageHostName);
				}
				catch(...)
				{
					cout << "ERROR: Problem setting Host Name in the Calpont System Configuration file" << endl;
					exit(-1);
				}
			}

			//get Network IP Address
			string IPAddress = oam.getIPAddress( newStorageHostName);
			if ( !IPAddress.empty() ) {
				newStorageIPAddr = IPAddress;

				cout << "'" << newStorageHostName << "' DNS IP Address is '" << newStorageIPAddr << "'" << endl;
			}
			else
			{ //no DNS address found for Host Name
				while (true)
				{
					prompt = "Enter IP Address of (" + storageIPAddr + ") > ";
					pcommand = readline(prompt.c_str());
					if (!pcommand)
						newStorageIPAddr = storageIPAddr;
					else
					{
						if (strlen(pcommand) > 0) newStorageIPAddr = pcommand;	
						free(pcommand);
						pcommand = 0;
					}
					if (oam.isValidIP(newStorageIPAddr))
						break;
					else
						cout << "Invalid IP Address format, xxx.xxx.xxx.xxx, please reenter" << endl;
				}
			}

			//set New Storage IP address
			string storageIPAddrNameParm = "StorageIPAddr" + oam.itoa(storageID) + "-" + oam.itoa(i+1);
			try {
				sysConfig->setConfig(StorageSection, storageIPAddrNameParm, newStorageIPAddr);
			}
			catch(...)
			{
				cout << "ERROR: Problem setting IP address in the Calpont System Configuration file" << endl;
				exit(-1);
			}
		} //end of while loop
	} //end of i for loop

	sysConfig->write();

	//
	// Configure NMS Addresses
	//

	cout << endl << "===== Setup the External Network Management System (NMS) Server Configuration =====" << endl << endl;

	string NMSIPAddress;
	try {
		NMSIPAddress = sysConfig->getConfig(SystemSection, "NMSIPAddress");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting NMSIPAddress from Calpont System Configuration file" << endl;
		exit(-1);
	}

	prompt = "Enter IP Address(es) of NMS Server (" + NMSIPAddress + ") > ";
	pcommand = readline(prompt.c_str());
	if (pcommand)
	{
		if (strlen(pcommand) > 0) NMSIPAddress = pcommand;
		free(pcommand);
		pcommand = 0;
	}

	try {
		sysConfig->setConfig(SystemSection, "NMSIPAddress", NMSIPAddress);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting NMSIPAddress in the Calpont System Configuration file" << endl;
		exit(-1);
	}


	//
	// setup TransactionArchivePeriod
	//

	cout << endl << "===== Setup the Transaction Log Archive Time Period  =====" << endl << endl;

	string transactionArchivePeriod;
	try {
		transactionArchivePeriod = sysConfig->getConfig(SystemSection, "TransactionArchivePeriod");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting transactionArchivePeriod from Calpont System Configuration file" << endl;
		exit(-1);
	}

	prompt = "Enter Transaction Archive Period in minutes (" + transactionArchivePeriod + ") > ";
	pcommand = readline(prompt.c_str());
	if (pcommand)
	{
		if (strlen(pcommand) > 0) transactionArchivePeriod = pcommand;
		free(pcommand);
		pcommand = 0;
	}

	try {
		sysConfig->setConfig(SystemSection, "TransactionArchivePeriod", transactionArchivePeriod);
	}
	catch(...)
	{
		cout << "ERROR: Problem setting IP address in the Calpont System Configuration file" << endl;
		exit(-1);
	}

	//
	//Update oidbitmap in Calpont.xml
	//

	try {
		sysConfig->setConfig("OIDManager", "OIDBitmapFile", "/mnt/OAM/dbrm/oidbitmap");
	}
	catch(...)
	{
		cout << "ERROR: Problem setting OIDBitmapFile Calpont System Configuration file" << endl;
		exit(-1);
	}

	//Write out Updated System Configuration File
	sysConfig->write();

	cout << endl << "Configure is successfuly completed, Calpont.xml is located in systems/" + systemName << endl << endl;
}
