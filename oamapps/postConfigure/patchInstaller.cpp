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
* $Id: patchInstaller.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
* Installs Calpont Software Patches on Calpont System
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

#include "liboamcpp.h"
#include "configcpp.h"
#include "snmpmanager.h"

using namespace std;
using namespace oam;
using namespace config;

bool installParentOAM( string patchLocation, string installLocation, string softwareFile );

typedef std::vector<string> Devices;

typedef struct Child_Module_struct
{
	std::string     moduleName;
	std::string     moduleIP;
	std::string     hostName;
} ChildModule;

typedef std::vector<ChildModule> ChildModuleList;

int main(int argc, char *argv[])
{
    Oam oam;
	string parentOAMModuleIPAddr;
	ChildModuleList childmodulelist;
	ChildModule childmodule;
	string prompt;
	string installer_debug = "0";

	Config* sysConfig = Config::makeConfig();
	string SystemSection = "SystemConfig";

	string patchLocation = argv[1];
	string installLocation = argv[2];
	string softwareFile = argv[3];
	string password = argv[4];
	installer_debug = argv[5];

	//get Parent OAM Module Name
	string parentOAMModuleName;
	try{
		parentOAMModuleName = sysConfig->getConfig(SystemSection, "ParentOAMModuleName");
	}
	catch(...)
	{
		cout << "ERROR: Problem getting Parent OAM Module Name" << endl;
		exit(-1);
	}

	//install patch on Parent OAM Module
	cout << endl << "----- Performing Patch installation on Controller OAM Module -----" << endl;
	if ( !installParentOAM( patchLocation, installLocation, softwareFile ) )
		cout << "Install Patch on Parent OAM Module error" << endl;
	else
		cout << "!!!Patch Installation on Controller OAM Module Successfully Completed" << endl;

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

	string ModuleSection = "SystemModuleConfig";

	for ( unsigned int i = 0 ; i < sysModuleTypeConfig.moduletypeconfig.size(); i++)
	{
		int moduleCount = sysModuleTypeConfig.moduletypeconfig[i].ModuleCount;

		if ( moduleCount == 0 )
			//no modules equipped for this Module Type, skip
			continue;

		//get IP addresses and Host Names
		for( int k=0 ; k < moduleCount ; k++ )
		{
			DeviceNetworkList::iterator listPT = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for( ; listPT != sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; listPT++)
			{
				string moduleName = (*listPT).DeviceName;
				HostConfigList::iterator pt1 = (*listPT).hostConfigList.begin();
				string moduleIPAddr = (*pt1).IPAddr;

				if ( (*pt1).IPAddr == oam::UnassignedIpAddr)
					// skip, unassigned server
					continue;
	
				//Install Software Patch on non Parent OAM Modules
				if ( moduleName != parentOAMModuleName ) {
					//run remote patch installer script
					cout << endl << "----- Performing Patch installation of Module '" + moduleName + "' -----" << endl << endl;
					string cmd = "/usr/local/Calpont/bin/patch_installer.sh " + moduleName + " " + moduleIPAddr + " " + password + " " + patchLocation + " " + installLocation + " " + softwareFile + " " + installer_debug;

					int rtnCode = system(cmd.c_str());
					if (rtnCode != 0)
						cout << "Error with running patch_installer.sh" << endl;
				}
			}
		} // end of k loop
	} //end of i for loop

}

/*
 * Install Software File patch on local Parent OAM Module
 */

bool installParentOAM( string patchLocation, string installLocation, string softwareFile )
{
	// Rename current file
	string cmd = "mv " + installLocation + softwareFile + " " + installLocation + softwareFile + ".patchSave";
	int rtnCode = system(cmd.c_str());
	if (rtnCode != 0) {
		cout << "Error save current file" << endl;
		return false;
	}

	// Install patch file
	cmd = "cp " + patchLocation + softwareFile + " " + installLocation + softwareFile;
	rtnCode = system(cmd.c_str());
	if (rtnCode != 0) {
		cout << "Error copying patch file" << endl;
		return false;
	}

	return true;
}
