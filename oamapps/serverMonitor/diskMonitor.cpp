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
 * $Id: diskMonitor.cpp 34 2006-09-29 21:13:54Z dhill $
 *
 *   Author: Zhixuan Zhu
 ***************************************************************************/

#include "serverMonitor.h"

using namespace std;
using namespace oam;
using namespace snmpmanager;
using namespace logging;
using namespace servermonitor;
using namespace config;
//using namespace procheartbeat;

SystemDiskList sdl;

typedef struct DBrootData_struct
{
	std::string     dbrootDir;
	bool            downFlag;
}
DBrootData;

typedef std::vector<DBrootData> DBrootList;


/*****************************************************************************************
* @brief	diskMonitor Thread
*
* purpose:	Get current Local and External disk usage and report alarms
*
*****************************************************************************************/
void diskMonitor()
{
	ServerMonitor serverMonitor;
	Oam oam;
    SystemConfig systemConfig;
	ModuleTypeConfig moduleTypeConfig;
	typedef std::vector<std::string> LocalFileSystems;
	LocalFileSystems lfs;
	struct statvfs buf; 

	// set defaults
	int localDiskCritical = 90,
		localDiskMajor = 80,
		localDiskMinor = 70,
		ExternalDiskCritical = 90,
		ExternalDiskMajor = 80,
		ExternalDiskMinor = 70;

	// get module types
	string moduleType;
	int moduleID=-1;
	string moduleName;
	oamModuleInfo_t t;
	try {
		t = oam.getModuleInfo();
		moduleType = boost::get<1>(t);
		moduleID = boost::get<2>(t);
		moduleName = boost::get<0>(t);
	}
	catch (exception& e) {}

	bool Externalflag = false;

	//check for external disk
	DBrootList dbrootList;
	if (moduleType == "pm") {
		systemStorageInfo_t t;
		t = oam.getStorageConfig();
		if ( boost::get<0>(t) == "external")
			Externalflag = true;

		// get dbroot list and storage type from config file
		DBRootConfigList dbrootConfigList;
		oam.getPmDbrootConfig(moduleID, dbrootConfigList);
	
		DBRootConfigList::iterator pt = dbrootConfigList.begin();
		for( ; pt != dbrootConfigList.end() ; pt++)
		{
			int dbrootID = *pt;
	
			string dbroot = "DBRoot" + oam.itoa(dbrootID);
	
			string dbootdir;
			try{
				oam.getSystemConfig(dbroot, dbootdir);
			}
			catch(...) {}
	
			if ( dbootdir.empty() || dbootdir == "" )
				continue;
	
			DBrootData dbrootData;
			dbrootData.dbrootDir = dbootdir;
			dbrootData.downFlag = false;
	
			dbrootList.push_back(dbrootData);
		}
	}

	string cloud = oam::UnassignedName;
	try {
		oam.getSystemConfig( "Cloud", cloud);
	}
	catch(...) {
		cloud = oam::UnassignedName;
	}

	//get Gluster Config setting
	string GlusterConfig = "n";
	try {
		oam.getSystemConfig( "GlusterConfig", GlusterConfig);
	}
	catch(...)
	{
		GlusterConfig = "n";
	}

	int diskSpaceCheck = 0;

	while(true)
	{
		SystemStatus systemstatus;
		try {
			oam.getSystemStatus(systemstatus);
		}
		catch (exception& ex)
		{}
		
		if (systemstatus.SystemOpState != oam::ACTIVE ) {
			sleep(5);
			continue;
		}

		// Get Local/External Disk Mount points to monitor and associated thresholds
		
		try {
			oam.getSystemConfig (moduleTypeConfig);
			localDiskCritical = moduleTypeConfig.ModuleDiskCriticalThreshold; 
			localDiskMajor = moduleTypeConfig.ModuleDiskMajorThreshold; 
			localDiskMinor = moduleTypeConfig.ModuleDiskMinorThreshold;

			DiskMonitorFileSystems::iterator p = moduleTypeConfig.FileSystems.begin();
			for( ; p != moduleTypeConfig.FileSystems.end() ; p++)
			{
				string fs = *p;
				lfs.push_back(fs);

				if (DISK_DEBUG) {
					//Log this event 
					LoggingID lid(SERVER_MONITOR_LOG_ID);
					MessageLog ml(lid);
					Message msg;
					Message::Args args;
					args.add("Local Config File System to monitor =");
					args.add(fs);
					msg.format(args);
					ml.logDebugMessage(msg);
				}
			}

		} catch (...)
		{
			sleep(5);
			continue;
		}

		// get External info
		try
		{
			oam.getSystemConfig(systemConfig);

		} catch (...)
		{
			sleep(5);
			continue;
		}

		if (Externalflag) {
			// get External info
			try
			{
				ExternalDiskCritical = systemConfig.ExternalCriticalThreshold;
				ExternalDiskMajor = systemConfig.ExternalMajorThreshold;
				ExternalDiskMinor = systemConfig.ExternalMinorThreshold;

			} catch (...)
			{
				sleep(5);
				continue;
			}
		}

		//check for local file systems
		LocalFileSystems::iterator p = lfs.begin();
		while(p != lfs.end())
		{
			string deviceName = *p;
			++p;
			string fileName;
			// check local
			if ( deviceName == "/") {
				fileName = deviceName + "usr/local/Calpont/releasenum";
			}
			else
			{
				fileName = deviceName + "/000.dir";
			}

			uint64_t totalBlocks;
			uint64_t usedBlocks;

			if (!statvfs(fileName.c_str(), &buf)) {

				uint64_t blksize, blocks, freeblks, free; 

				blksize = buf.f_bsize; 
				blocks = buf.f_blocks; 
				freeblks = buf.f_bfree; 

				totalBlocks = blocks * blksize;
				free = freeblks * blksize; 
				usedBlocks = totalBlocks - free; 
			}
			else
				continue;

			int64_t diskUsage = 0;
			if ( totalBlocks == 0 ) {
				diskUsage = 0;
	
				//Log this event 
				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("Total Disk Usage is set to 0");
				msg.format(args);
				ml.logWarningMessage(msg);
			}
			else
				diskUsage =  (usedBlocks / (totalBlocks / 100)) + 1;

			SMSystemDisk sd;
			sd.deviceName = deviceName;
			sd.usedPercent = diskUsage;
			sd.totalBlocks = totalBlocks;
			sd.usedBlocks = usedBlocks;
			sdl.push_back(sd);

			if (DISK_DEBUG)
				cout << "Disk Usage for " << deviceName << " is " << diskUsage << endl;
	
			if ( diskSpaceCheck == 0 )
			{
				if (diskUsage >= localDiskCritical && localDiskCritical > 0 ) {
					//adjust if over 100%
					if ( diskUsage > 100 )
						diskUsage = 100;
					if ( serverMonitor.sendResourceAlarm(deviceName, DISK_USAGE_HIGH, SET, (int) diskUsage) )
					{
						LoggingID lid(SERVER_MONITOR_LOG_ID);
						MessageLog ml(lid);
						Message msg;
						Message::Args args;
						args.add("Local Disk above Critical Disk threshold with a percentage of ");
						args.add((int) diskUsage);
						msg.format(args);
						ml.logInfoMessage(msg);
					}
				}
				else if (diskUsage >= localDiskMajor && localDiskMajor > 0 ) {
					if (serverMonitor.sendResourceAlarm(deviceName, DISK_USAGE_MED, SET, (int) diskUsage))
					{
						LoggingID lid(SERVER_MONITOR_LOG_ID);
						MessageLog ml(lid);
						Message msg;
						Message::Args args;
						args.add("Local Disk above Major Disk threshold with a percentage of ");
						args.add((int) diskUsage);
						msg.format(args);
						ml.logInfoMessage(msg);
					}
				}
				else if (diskUsage >= localDiskMinor && localDiskMinor > 0 ) {
					if ( serverMonitor.sendResourceAlarm(deviceName, DISK_USAGE_LOW, SET, (int) diskUsage))
					{
						LoggingID lid(SERVER_MONITOR_LOG_ID);
						MessageLog ml(lid);
						Message msg;
						Message::Args args;
						args.add("Local Disk above Minor Disk threshold with a percentage of ");
						args.add((int) diskUsage);
						msg.format(args);
						ml.logInfoMessage(msg);
					}
				}
				else
					serverMonitor.checkDiskAlarm(deviceName);
			}
	
			//check for external file systems/devices
			if (Externalflag ||
				(!Externalflag && GlusterConfig == "y" && moduleType == "pm") ){
				try
				{
					DBRootConfigList dbrootConfigList;
					oam.getPmDbrootConfig(moduleID, dbrootConfigList);
	
					DBRootConfigList::iterator pt = dbrootConfigList.begin();
					for( ; pt != dbrootConfigList.end() ; pt++)
					{
						int dbroot = *pt;
						string deviceName = systemConfig.DBRoot[dbroot-1];
						string fileName = deviceName + "/000.dir";
			
						if (DISK_DEBUG) {
							//Log this event 
							LoggingID lid(SERVER_MONITOR_LOG_ID);
							MessageLog ml(lid);
							Message msg;
							Message::Args args;
							args.add("DBRoots monitoring");
							args.add(dbroot);
							args.add(" ,file system =" );
							args.add(fileName);
							msg.format(args);
							ml.logDebugMessage(msg);
						}
	
						uint64_t totalBlocks;
						uint64_t usedBlocks;
			
						if (!statvfs(fileName.c_str(), &buf)) {
			
							uint64_t blksize, blocks, freeblks, free; 
			
							blksize = buf.f_bsize; 
							blocks = buf.f_blocks; 
							freeblks = buf.f_bfree; 
			
							totalBlocks = blocks * blksize;
							free = freeblks * blksize; 
							usedBlocks = totalBlocks - free; 
						}
						else
						{
							SMSystemDisk sd;
							sd.deviceName = deviceName;
							sd.usedPercent = 0;
							sd.totalBlocks = 0;
							sd.usedBlocks = 0;
							sdl.push_back(sd);
							continue;
						}
			
						int diskUsage = 0;
						if ( totalBlocks == 0 ) {
							diskUsage = 0;
				
							//Log this event 
							LoggingID lid(SERVER_MONITOR_LOG_ID);
							MessageLog ml(lid);
							Message msg;
							Message::Args args;
							args.add("Total Disk Usage is set to 0");
							msg.format(args);
							ml.logWarningMessage(msg);
						}
						else
							diskUsage =  (usedBlocks / (totalBlocks / 100)) + 1;
			
						SMSystemDisk sd;
						sd.deviceName = deviceName;
						sd.usedPercent = diskUsage;
						sd.totalBlocks = totalBlocks;
						sd.usedBlocks = usedBlocks;
						sdl.push_back(sd);
		
						if (DISK_DEBUG)
							cout << "Disk Usage for " << deviceName << " is " << diskUsage << endl;
			
						if (diskUsage >= ExternalDiskCritical && ExternalDiskCritical > 0 ) {
							//adjust if over 100%
							if ( diskUsage > 100 )
								diskUsage = 100;
							if ( serverMonitor.sendResourceAlarm(deviceName, DISK_USAGE_HIGH, SET, diskUsage))
							{
								LoggingID lid(SERVER_MONITOR_LOG_ID);
								MessageLog ml(lid);
								Message msg;
								Message::Args args;
								args.add("Disk usage for");
								args.add(deviceName);
								args.add(" above Critical Disk threshold with a percentage of ");
								args.add((int) diskUsage);
								msg.format(args);
								ml.logInfoMessage(msg);
							}
						}
						else if (diskUsage >= ExternalDiskMajor && ExternalDiskMajor > 0 ) {
							if ( serverMonitor.sendResourceAlarm(deviceName, DISK_USAGE_MED, SET, diskUsage))
							{
								LoggingID lid(SERVER_MONITOR_LOG_ID);
								MessageLog ml(lid);
								Message msg;
								Message::Args args;
								args.add("Disk usage for");
								args.add(deviceName);
								args.add(" above Major Disk threshold with a percentage of ");
								args.add((int) diskUsage);
								msg.format(args);
								ml.logInfoMessage(msg);
							}
						}
						else if (diskUsage >= ExternalDiskMinor && ExternalDiskMinor > 0 ) {
							if ( serverMonitor.sendResourceAlarm(deviceName, DISK_USAGE_LOW, SET, diskUsage))
							{
								LoggingID lid(SERVER_MONITOR_LOG_ID);
								MessageLog ml(lid);
								Message msg;
								Message::Args args;
								args.add("Disk usage for");
								args.add(deviceName);
								args.add(" above Minor Disk threshold with a percentage of ");
								args.add((int) diskUsage);
								msg.format(args);
								ml.logInfoMessage(msg);
							}
						}
						else
							serverMonitor.checkDiskAlarm(deviceName);
					}
				}
				catch (exception& e)
				{
					cout << endl << "**** getPmDbrootConfig Failed :  " << e.what() << endl;
				}
			}
		}

		//check OAM dbroot test flag to validate dbroot exist if on pm
		if ( moduleName.find("pm") != string::npos ) {
			//check OAM dbroot test flag to validate dbroot exist
			if ( dbrootList.size() != 0 ) {
				DBrootList::iterator p = dbrootList.begin();
				while ( p != dbrootList.end() )
				{
					//get dbroot directory
					string dbrootDir = (*p).dbrootDir;
					string dbrootName;
					string dbrootID;

					//get dbroot name
					string::size_type pos = dbrootDir.rfind("/",80);
					if (pos != string::npos)
						dbrootName = dbrootDir.substr(pos+1,80);

					//get ID
					dbrootID = dbrootName.substr(4,80);
			
					string fileName = dbrootDir + "/OAMdbrootCheck";
					// retry in case we hit the remount window
					for ( int retry = 0 ; ; retry++ )
					{
						bool fail = false;
						//first test, check if OAMdbrootCheck exists
						ifstream file (fileName.c_str());
						if (!file)
							fail = true;
						else
						{	//second test for amazon, check volume status
							if ( cloud != oam::UnassignedName ) {
								string volumeNameID = "PMVolumeName" + dbrootID;
								string volumeName = oam::UnassignedName;
								try {
									oam.getSystemConfig( volumeNameID, volumeName);
								}
								catch(...)
								{}
							
								if ( volumeName.empty() || volumeName == oam::UnassignedName )
									fail = false;
								else
								{
									string status = oam.getEC2VolumeStatus(volumeName);
									if ( status == "attached" )
										fail = false;
									else
									{
										fail = true;
										LoggingID lid(SERVER_MONITOR_LOG_ID);
										MessageLog ml(lid);
										Message msg;
										Message::Args args;
										args.add("dbroot monitoring: Volume not attached");
										args.add(volumeName);
										args.add("/");
										args.add(dbrootName);
										msg.format(args);
										ml.logCriticalMessage(msg);
									}
								}
							}
							else
								fail = false;
						}

						if (fail) {
							//double check system status before reporting any error BUG 5078
							SystemStatus systemstatus;
							try {
								oam.getSystemStatus(systemstatus);
							}
							catch (exception& ex)
							{}
							
							if (systemstatus.SystemOpState != oam::ACTIVE ) {
								break;
							}

							if ( retry < 10 ) {
								sleep(3);
								continue;
							}
							else
							{
								if ( !(*p).downFlag ) {
									LoggingID lid(SERVER_MONITOR_LOG_ID);
									MessageLog ml(lid);
									Message msg;
									Message::Args args;
									args.add("dbroot monitoring: Lost access to ");
									args.add(dbrootDir);
									msg.format(args);
									ml.logCriticalMessage(msg);

									oam.sendDeviceNotification(dbrootName, DBROOT_DOWN, moduleName);
									(*p).downFlag = true;

									try{
										oam.setDbrootStatus(dbrootID, oam::AUTO_OFFLINE);
									}
									catch (exception& ex)
									{}

									break;
								}
							}
						}
						else
						{
							if ( (*p).downFlag ) {
								LoggingID lid(SERVER_MONITOR_LOG_ID);
								MessageLog ml(lid);
								Message msg;
								Message::Args args;
								args.add("dbroot monitoring: Access back to ");
								args.add(dbrootDir);
								msg.format(args);
								ml.logInfoMessage(msg);
		
								oam.sendDeviceNotification(dbrootName, DBROOT_UP, moduleName);
								(*p).downFlag = false;

								try{
									oam.setDbrootStatus(dbrootID, oam::ACTIVE);
								}
								catch (exception& ex)
								{}
							}
							file.close();
							break;
						}
					}
					p++;
				}
			}
		}

		//do Gluster status check, if configured
		if ( GlusterConfig == "y")
		{
			bool pass = true;
			string errmsg = "unknown";
			try {
				string arg1 = "";
				string arg2 = "";
				int ret = oam.glusterctl(oam::GLUSTER_STATUS, arg1, arg2, errmsg);
				if ( ret != 0 )
				{
					cerr << "FAILURE: Status check error: " + errmsg << endl;
					pass = false;
				}
			}
			catch (exception& e)
			{
				cerr << endl << "**** glusterctl API exception:  " << e.what() << endl;
				cerr << "FAILURE: Status check error" << endl;
				pass = false;
			}
			catch (...)
			{
				cerr << endl << "**** glusterctl API exception: UNKNOWN" << endl;
				cerr << "FAILURE: Status check error" << endl;
				pass = false;
			}

			if ( !pass )
			{ // issue log and alarm
				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("Gluster Status check failure error msg: ");
				args.add(errmsg);
				msg.format(args);
				ml.logWarningMessage(msg);
				serverMonitor.sendResourceAlarm(errmsg, GLUSTER_DISK_FAILURE, SET, 0);
			}
		}

		// sleep 10 seconds
		sleep(MONITOR_PERIOD/6);

		//check disk space every 10 minutes
		diskSpaceCheck++;
		if ( diskSpaceCheck >= 60 )
			diskSpaceCheck = 0;

		lfs.clear();
		sdl.clear();

	} // end of while loop
}

/******************************************************************************************
* @brief	checkDiskAlarm
*
* purpose:	check to see if an alarm(s) is set on Disk and clear if so
*
******************************************************************************************/
void ServerMonitor::checkDiskAlarm(string alarmItem, ALARMS alarmID)
{
	Oam oam;
	ServerMonitor serverMonitor;

	// get current server name
	string serverName;
	oamModuleInfo_t st;
	try {
		st = oam.getModuleInfo();
		serverName = boost::get<0>(st);
	}
	catch (...) {
		serverName = "Unknown Server";
	}

	switch (alarmID) {
		case NO_ALARM: 	// clear all alarms set if any found
			if ( oam.checkActiveAlarm(DISK_USAGE_HIGH, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, DISK_USAGE_HIGH);
			if ( oam.checkActiveAlarm(DISK_USAGE_MED, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, DISK_USAGE_MED);
			if ( oam.checkActiveAlarm(DISK_USAGE_LOW, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, DISK_USAGE_LOW);
			break;
		case DISK_USAGE_LOW: 	// clear high and medium alarms set if any found
			if ( oam.checkActiveAlarm(DISK_USAGE_HIGH, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, DISK_USAGE_HIGH);
			if ( oam.checkActiveAlarm(DISK_USAGE_MED, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, DISK_USAGE_MED);
			break;
		case DISK_USAGE_MED: 	// clear high alarms set if any found
			if ( oam.checkActiveAlarm(DISK_USAGE_HIGH, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, DISK_USAGE_HIGH);
			break;
		default:			// none to clear
			break;
		} // end of switch
	return;
}
