/******************************************************************************************
* $Id: queryStats.cpp 64 2006-10-12 22:21:51Z dhill $
*
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
#include <time.h>

#include <readline/readline.h>
#include <readline/history.h>

#include "liboamcpp.h"
#include "configcpp.h"

using namespace std;
using namespace oam;
using namespace config;

typedef std::vector<string> StorageDevices;

typedef struct Child_Module_struct
{
	std::string     moduleName;
	std::string     moduleIP;
	std::string     hostName;
} ChildModule;

typedef std::vector<ChildModule> ChildModuleList;

typedef struct Performance_Module_struct
{
	std::string     moduleName;
	std::string     moduleIP;
	std::string     hostName;
} PerformanceModule;

typedef std::vector<PerformanceModule> PerformanceModuleList;

int main(int argc, char *argv[])
{
    Oam oam;
    char* pcommand = 0;
	StorageDevices storageDevices;
	ChildModuleList childmodulelist;
	ChildModule childmodule;
	PerformanceModuleList performancemodulelist;
	PerformanceModule performancemodule;
	SystemConfig systemconfig;
	string softwareRelease;
	string parentOAMModule;
	string parentHostName;
	SystemSoftware systemsoftware;

	string prompt;

	string commandType = "";
	string queryType = "";
	string runNumber = "";
	string password = "";
	string remoteDebugFlag = "0";

    if (argc > 1)
		commandType = argv[1];
    if (argc > 2)
		queryType = argv[2];
    if (argc > 3)
		runNumber = argv[3];
    if (argc > 4)
		password = argv[4];
    if (argc > 5)
		remoteDebugFlag = argv[5];

	if ( commandType.empty() ) {
		pcommand = readline("Enter QueryStats Command (start or stop) > ");
		if (pcommand)
		{
			commandType = pcommand;
			free(pcommand);
			pcommand = 0;
		}
	}

	if ( queryType.empty() ) {
		pcommand = readline("Enter Query being executed > ");
		if (pcommand)
		{
			queryType = pcommand;
			free(pcommand);
			pcommand = 0;
		}
	}

	if ( runNumber.empty() ) {
		pcommand = readline("Enter Run Number > ");
		if (pcommand)
		{
			runNumber = pcommand;
			free(pcommand);
			pcommand = 0;
		}
	}

	runNumber = "run" + runNumber;

	if ( password.empty() ) {
		pcommand = readline("Enter root password > ");
		if (pcommand)
		{
			password = pcommand;
			free(pcommand);
			pcommand = 0;
		}
	}

	string testName = queryType + "-" + runNumber;

	try{
		oam.getSystemConfig("ParentOAMModuleName", parentOAMModule);
	}
	catch(...)
	{
		cout << endl << "**** Failed : Failed to read Parent OAM Module Name" << endl;
		exit(-1);
	}

	try
	{
		oam.getSystemSoftware(systemsoftware);
		softwareRelease = "rel" + systemsoftware.Release;
	}
	catch(...)
	{
		cout << endl << "**** Failed : Failed to read getSystemSoftware" << endl;
		exit(-1);
	}

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

	// get Data storage Mount
	Config* sysConfig = Config::makeConfig();
	string SystemSection = "SystemConfig";

	int DBRootCount;

	try {
		DBRootCount = strtol(sysConfig->getConfig(SystemSection, "DBRootCount").c_str(), 0, 0);
	}
	catch(...)
	{
		cout << "ERROR: Problem getting DB Storage Data from the Calpont System Configuration file" << endl;
		exit(-1);
	}

	for( int i=1 ; i < DBRootCount+1 ; i++)
	{
		string DBRootStorageLocID = "DBRootStorageLoc" + oam.itoa(i);
		try {
			string DBRootStorageLoc = sysConfig->getConfig("Installation", DBRootStorageLocID);

			DBRootStorageLoc = DBRootStorageLoc.substr(5,3);
			storageDevices.push_back(DBRootStorageLoc);
		}
		catch(...)
		{
			cout << "ERROR: Problem getting DB Storage Data from the Calpont System Configuration file" << endl;
			exit(-1);
		}
	}

	// get modules in system
	for ( unsigned int i = 0 ; i < sysModuleTypeConfig.moduletypeconfig.size(); i++)
	{
		string moduleType = sysModuleTypeConfig.moduletypeconfig[i].ModuleType;
		int moduleCount = sysModuleTypeConfig.moduletypeconfig[i].ModuleCount;

		if ( moduleCount == 0 )
			//no modules equipped for this Module Type, skip
			continue;

		//get IP addresses
		for( int k=0 ; k < moduleCount ; k++ )
		{
			int moduleID = k+1;
			string moduleName = moduleType + oam.itoa(moduleID);

			DeviceNetworkList::iterator listPT = sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.begin();
			for( ; listPT != sysModuleTypeConfig.moduletypeconfig[i].ModuleNetworkList.end() ; listPT++)
			{
				if ( moduleName == (*listPT).DeviceName &&
					moduleName != parentOAMModule ) {
					
					childmodule.moduleName = moduleName;
					childmodule.moduleIP = (*listPT).hostConfigList[0].IPAddr;
					childmodule.hostName = (*listPT).hostConfigList[0].HostName + "-" + moduleName;
					childmodulelist.push_back(childmodule);

					if ( moduleType == "pm" ) {
						performancemodule.moduleName = moduleName;
						performancemodule.moduleIP = (*listPT).hostConfigList[0].IPAddr;
						performancemodule.hostName = (*listPT).hostConfigList[0].HostName + "-" + moduleName;
						performancemodulelist.push_back(performancemodule);
					}
					break;
				}
				else
					if ( moduleName == parentOAMModule )
						parentHostName = (*listPT).hostConfigList[0].HostName;
			}
		} //end of k (moduleCount) loop
	} //end of i for loop

	//get current times
	time_t ttNow;
	time(&ttNow);
	tm tmNow;
	localtime_r(&ttNow, &tmNow);
	string month = oam.itoa(tmNow.tm_mon+1);
	if ( month.size() == 1 )
		month = "0" + month;
	string day = oam.itoa(tmNow.tm_mday);
	if ( day.size() == 1 )
		day = "0" + day;
	string hour = oam.itoa(tmNow.tm_hour);
	if ( hour.size() == 1 )
		hour = "0" + hour;
	string minutes = oam.itoa(tmNow.tm_min);
	if ( minutes.size() == 1 )
		minutes = "0" + minutes;
	string seconds = oam.itoa(tmNow.tm_sec);
	if ( seconds.size() == 1 )
		seconds = "0" + seconds;

	string Date = "date" + month + day;
	string currentTime = hour + ":" + minutes + ":" + seconds;

	//setup directories
	string iostatsDir = "/home/qa/stats/" + softwareRelease + "/" + Date + "/" + runNumber + "/";
	string iostatsDirCmd = "mkdir -p " + iostatsDir;
	string iostatsFile = "diskdata_" + testName + ".txt";
	string sarDir = "/home/qa/stats/" + softwareRelease + "/" + Date + "/";
	string sarDirCmd = "mkdir -p " + sarDir;
	string sarFile = "sardata_" + testName + ".txt";
	string remoteCmd = "/usr/local/Calpont/bin/remote_command.sh ";

	if ( commandType == "start" )
	{
		// send command to setup and start iostat on all PMs

		string iostatsCmd = "iostat -dmt 2 -x ";

		StorageDevices::iterator list = storageDevices.begin();
		for (; list != storageDevices.end() ; list++)
		{
			iostatsCmd = iostatsCmd + *list + " ";
		}
		iostatsCmd = iostatsCmd + "\\> " + iostatsDir + iostatsFile;

		PerformanceModuleList::iterator list1 = performancemodulelist.begin();
		for (; list1 != performancemodulelist.end() ; list1++)
		{
			//run remote command script to create iostats directory
			string cmd = remoteCmd + (*list1).moduleIP + " " + password + " '" + iostatsDirCmd  + "' " + remoteDebugFlag;
			int rtnCode = system(cmd.c_str());
			if (rtnCode != 0)
				cout << "Error with running remote_command.sh" << endl;

			//run remote command script to start iostats
			cmd = remoteCmd + (*list1).moduleIP + " " + password + " '" + iostatsCmd + "' " + remoteDebugFlag;
			rtnCode = system(cmd.c_str());
			if (rtnCode != 0)
				cout << "Error with running remote_command.sh" << endl;
		}

		//store start time
		string cmd = "echo " + currentTime + " > /tmp/queryStart";
		int rtnCode = system(cmd.c_str());
		if (rtnCode != 0)
			cout << "Error outputting current time" << endl;
		
		cout << "Query-Stats 'Start' Successfully Completed, run query now!!!" << endl;
	}
	else // stop command
	{
		// get query start time
		string fileName = "/tmp/queryStart";
	
		ifstream file (fileName.c_str());
		if (!file) exit(-1);
		
		char stime[200];
		file.getline(stime, 200);
		string startTime = stime;

		// send command to stop iostat on all PMs
		string iostatsCmd = "pkill -9 iostat";

		PerformanceModuleList::iterator list1 = performancemodulelist.begin();
		for (; list1 != performancemodulelist.end() ; list1++)
		{
			string cmd = remoteCmd + (*list1).moduleIP + " " + password + " '" + iostatsCmd + "' " + remoteDebugFlag;
			int rtnCode = system(cmd.c_str());
			if (rtnCode != 0)
				cout << "Error with running remote_command.sh" << endl;
		}

		//create local sar directory
		int rtnCode = system(sarDirCmd.c_str());
		if (rtnCode != 0)
			cout << "Error with running remote_command.sh" << endl;

		//extract sar data on local server
		string sarCmd = "LC_ALL=C sar -A -s " + startTime + " -e " + currentTime + " -f /var/log/sa/sa" + day + " > " + sarDir + sarFile;
		rtnCode = system(sarCmd.c_str());
		if (rtnCode != 0)
			cout << "Error with running sar locally" << endl;
		
		//extract sar data on child modules
		ChildModuleList::iterator list2 = childmodulelist.begin();
		for (; list2 != childmodulelist.end() ; list2++)
		{
			//run remote command script to create sar directory
			string cmd = remoteCmd + (*list2).moduleIP + " " + password + " '" + sarDirCmd  + "' " + remoteDebugFlag;
			rtnCode = system(cmd.c_str());
			if (rtnCode != 0)
				cout << "Error with running remote_command.sh" << endl;

			sarCmd = "LC_ALL=C sar -A -s " + startTime + " -e " + currentTime + " -f /var/log/sa/sa" + day + " \\> " + sarDir + sarFile;
			cmd = remoteCmd + (*list2).moduleIP + " " + password + " '" + sarCmd + "' " + remoteDebugFlag;
			rtnCode = system(cmd.c_str());
			if (rtnCode != 0)
				cout << "Error with running remote_command.sh" << endl;
		}

		//create directories on shared server
		string sharedTopDir = "perfdata\\";
		string sharedRelDir = sharedTopDir + softwareRelease + "\\";
		string sharedDateDir = sharedRelDir + Date + "\\";
		string sharedTestDir = sharedDateDir + testName + "\\";
//		string sharedTestDir = sharedDateDir + testName + "-" + startTime + "-" + currentTime + "\\";
		string smbCmd1 = "smbclient //cal6500/shared -Wcalpont -U" + oam::USERNAME + "%" + oam::PASSWORD + " -c 'mkdir ";

		string smbCmd2 = smbCmd1 + sharedRelDir + "'";
		rtnCode = system(smbCmd2.c_str());
		if (rtnCode != 0)
			cout << "Error with running smbclient locally" << endl;

		smbCmd2 = smbCmd1 + sharedDateDir + "'";
		rtnCode = system(smbCmd2.c_str());
		if (rtnCode != 0)
			cout << "Error with running smbclient locally" << endl;

		smbCmd2 = smbCmd1 + sharedTestDir + "'";
		rtnCode = system(smbCmd2.c_str());
		if (rtnCode != 0)
			cout << "Error with running smbclient locally" << endl;

		smbCmd2 = smbCmd1 + sharedTestDir + parentHostName + "-dm1'";
		rtnCode = system(smbCmd2.c_str());
		if (rtnCode != 0)
			cout << "Error with running smbclient locally" << endl;

		list2 = childmodulelist.begin();
		for (; list2 != childmodulelist.end() ; list2++)
		{
			smbCmd2 = smbCmd1 + sharedTestDir + (*list2).hostName + "'";
			rtnCode = system(smbCmd2.c_str());
			if (rtnCode != 0)
				cout << "Error with running smbclient remotely" << endl;
		}

		//push local sar data to shared server
		smbCmd2 = "cd " + sarDir +";smbclient //cal6500/shared -Wcalpont -U" + oam::USERNAME + "%" + oam::PASSWORD + " -c 'cd " + sharedTestDir + parentHostName + "-dm1" + ";prompt OFF;put " + sarFile + "'";
		rtnCode = system(smbCmd2.c_str());
		if (rtnCode != 0)
			cout << "Error with running smbclient locally" << endl;

		//push child module sar data to shared server
		list2 = childmodulelist.begin();
		for (; list2 != childmodulelist.end() ; list2++)
		{
			smbCmd2 = "cd /mnt/" + (*list2).moduleName + sarDir +";smbclient //cal6500/shared -Wcalpont -U" + oam::USERNAME + "%" + oam::PASSWORD + " -c 'cd " + sharedTestDir + (*list2).hostName + ";prompt OFF;put " + sarFile + "'";
			rtnCode = system(smbCmd2.c_str());
			if (rtnCode != 0)
				cout << "Error with running smbclient locally" << endl;
		}

		//push performance module iostats data to shared server
		list1 = performancemodulelist.begin();
		for (; list1 != performancemodulelist.end() ; list1++)
		{
			smbCmd2 = "cd /mnt/" + (*list1).moduleName + iostatsDir +";smbclient //cal6500/shared -Wcalpont -U" + oam::USERNAME + "%" + oam::PASSWORD + " -c 'cd " + sharedTestDir + (*list1).hostName + ";prompt OFF;put " + iostatsFile + "'";
			rtnCode = system(smbCmd2.c_str());
			if (rtnCode != 0)
				cout << "Error with running smbclient locally" << endl;
		}

		cout << "Query-Stats 'Stop' Successfully Completed, data files located in cal6500/shared/" + sharedTestDir << endl;
	}

}
