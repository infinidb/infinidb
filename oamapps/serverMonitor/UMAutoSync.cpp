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
 * $Id: UMAutoSync.cpp 34 2006-09-29 21:13:54Z dhill $
 *
 *   Author: Zhixuan Zhu
 ***************************************************************************/

#include "serverMonitor.h"
#include "installdir.h"

using namespace std;
using namespace oam;
using namespace snmpmanager;
using namespace logging;
using namespace servermonitor;
using namespace config;

typedef struct UMmodule_struct
{
	std::string     moduleName;
	std::string     IPAddr;
} UMmodule;

typedef std::vector<UMmodule> UMmoduleList;

void rsync(std::string moduleName, std::string IPAddr, std::string rootPassword);


/*****************************************************************************************
* @brief	UMAutoSync Thread
*
* purpose:	check db health
*
*****************************************************************************************/
void UMAutoSync()
{
	ServerMonitor serverMonitor;
	Oam oam;
	UMmoduleList ummodulelist;

	// sync run time in minutes
	int UMSyncTime = 10;
	try {
		oam.getSystemConfig( "UMSyncTime", UMSyncTime);
	}
	catch(...) {
		UMSyncTime = 10;
	}

	if ( UMSyncTime < 1 )
		UMSyncTime = 10;

	//get root password
	string rootPassword = oam::UnassignedName;	// default to 'n'
	try {
		oam.getSystemConfig( "rpw", rootPassword);
	}
	catch(...) {
		rootPassword = oam::UnassignedName;
	}

	//if assigned, exit thread
	if (rootPassword == oam::UnassignedName)
		pthread_exit((void*) NULL);

	oamModuleInfo_t t;

	//get local module info
	string localModuleName;
	string localModuleType;

	try {
		t = oam.getModuleInfo();
		localModuleName = boost::get<0>(t);
		localModuleType = boost::get<1>(t);
	}
	catch (...) {}

	// loop forever
	while(true)
	{
//		LoggingID lid(SERVER_MONITOR_LOG_ID);
//		MessageLog ml(lid);
//		Message msg;
//		Message::Args args;
//		args.add("rsync thread running");
//		msg.format(args);
//		ml.logDebugMessage(msg);

		//
		// find non-disabled modules to sync up
		//
		try
		{
			SystemStatus systemstatus;
			oam.getSystemStatus(systemstatus);
			ummodulelist.clear();

			for( unsigned int i = 0 ; i < systemstatus.systemmodulestatus.modulestatus.size(); i++)
			{
				if( systemstatus.systemmodulestatus.modulestatus[i].Module.empty() )
					// end of list
					break;
	
				string moduleName =  systemstatus.systemmodulestatus.modulestatus[i].Module;
				string moduleType = moduleName.substr(0,MAX_MODULE_TYPE_SIZE);
				if ( moduleType == localModuleType && moduleName != localModuleName ) {
					int state = systemstatus.systemmodulestatus.modulestatus[i].ModuleOpState;
					if ( state == oam::MAN_DISABLED || state == oam::AUTO_DISABLED )
						continue;
					else {	//get module IP Address
						ModuleConfig moduleconfig;
						oam.getSystemConfig(moduleName, moduleconfig);
						HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();

						UMmodule ummodule;
				
						ummodule.IPAddr = (*pt1).IPAddr;
						ummodule.moduleName = moduleName;

						ummodulelist.push_back(ummodule);
					}
				}
			}
		}
		catch(...) {}

		//update all ums
		UMmoduleList::iterator list1 = ummodulelist.begin();
		for (; list1 != ummodulelist.end() ; list1++)
		{
			//call rsync function
			rsync((*list1).moduleName, (*list1).IPAddr, rootPassword);
		}

		//
		// go into check for um module update module, rsync to new modules
		//
		for ( int time = 0 ; time < UMSyncTime ; time++ )
		{
			try
			{
				SystemStatus systemstatus;
				oam.getSystemStatus(systemstatus);
	
				for( unsigned int i = 0 ; i < systemstatus.systemmodulestatus.modulestatus.size(); i++)
				{
					if( systemstatus.systemmodulestatus.modulestatus[i].Module.empty() )
						// end of list
						break;
		
					string moduleName =  systemstatus.systemmodulestatus.modulestatus[i].Module;
					string moduleType = moduleName.substr(0,MAX_MODULE_TYPE_SIZE);
					if ( moduleType == localModuleType && moduleName != localModuleName ) {
						int state = systemstatus.systemmodulestatus.modulestatus[i].ModuleOpState;
						if ( state == oam::MAN_DISABLED || state == oam::AUTO_DISABLED )
							continue;
						else {	//check if in current sync list
							UMmoduleList::iterator list1 = ummodulelist.begin();
							bool found = false;
							for (; list1 != ummodulelist.end() ; list1++)
							{
								if ( moduleName == (*list1).moduleName) {
									found = true;
									break;
								}
							}

							if ( !found) {
								//get module IP Address
								ModuleConfig moduleconfig;
								oam.getSystemConfig(moduleName, moduleconfig);
								HostConfigList::iterator pt1 = moduleconfig.hostConfigList.begin();
	
								//call rsync function
								rsync(moduleName, (*pt1).IPAddr, rootPassword);

								UMmodule ummodule;
						
								ummodule.IPAddr = (*pt1).IPAddr;
								ummodule.moduleName = moduleName;
		
								ummodulelist.push_back(ummodule);
							}
						}
					}
				}
			}
			catch(...) {}
	
			sleep(60);
		}

	} // end of while loop
}

/*
*	rsync script function
*/

void rsync(std::string moduleName, std::string IPAddr, std::string rootPassword)
{

	string cmd = startup::StartUp::installDir() + "/bin/rsync.sh " + IPAddr + " " + rootPassword + " 1 > /tmp/rsync_" + moduleName + ".log";
	int ret = system(cmd.c_str());

	if ( WEXITSTATUS(ret) == 0 )
	{
/*		LoggingID lid(SERVER_MONITOR_LOG_ID);
		MessageLog ml(lid);
		Message msg;
		Message::Args args;
		args.add("Successfully rsync to module: ");
		args.add(moduleName);
		msg.format(args);
		ml.logDebugMessage(msg);
*/	}
	else
	{
		LoggingID lid(SERVER_MONITOR_LOG_ID);
		MessageLog ml(lid);
		Message msg;
		Message::Args args;
		args.add("Failure rsync to module: ");
		args.add(moduleName);
		msg.format(args);
		ml.logDebugMessage(msg);

	}
	return;
}

