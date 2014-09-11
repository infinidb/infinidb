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
* purpose:	Get current Local and RAID disk usage and report alarms
*
*****************************************************************************************/
void diskMonitor()
{
	ServerMonitor serverMonitor;
	Oam oam;
    SystemConfig systemConfig;
	ModuleTypeConfig moduleTypeConfig;
	string RAIDfs;
	typedef std::vector<std::string> LocalFileSystems;
	LocalFileSystems lfs;
	struct statvfs buf; 
	// set defaults
	int localDiskCritical = 90,
		localDiskMajor = 80,
		localDiskMinor = 70,
		RAIDDiskCritical = 90,
		RAIDDiskMajor = 80,
		RAIDDiskMinor = 70;

	// get Master server flag, will decide to monitor RAID or not
	bool OAMParentFlag;
	string moduleName;
	oamModuleInfo_t st;
	try {
		st = oam.getModuleInfo();
		OAMParentFlag = boost::get<2>(st);
		moduleName = boost::get<0>(st);
	}
	catch (...) {
		// default to false
		OAMParentFlag = false;
	}

	bool Externalflag = false;

	//check for external disk
	if (OAMParentFlag) {
		Config* sysConfig = Config::makeConfig();
		if ( sysConfig->getConfig("Installation", "DBRootStorageType") == "storage" )
			Externalflag = true;
	}

	// get dbroot list and storage type from config file
	DBrootList dbrootList;
	for ( int id = 1 ;; id++ )
	{
		string dbroot = "DBRoot" + oam.itoa(id);

		string dbootD;
		try{
			oam.getSystemConfig(dbroot, dbootD);
		}
		catch(...) {}

		if ( dbootD.empty() || dbootD == "" )
			break;

		DBrootData dbrootData;
		dbrootData.dbrootDir = dbootD;
		dbrootData.downFlag = false;

		dbrootList.push_back(dbrootData);
	}

	while(true)
	{
		// Get Local/RAID Disk Mount points to monitor and associated thresholds
		
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

				//Log this event 
/*				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("Local Config File System to monitor =");
				args.add(fs);
				msg.format(args);
				ml.logDebugMessage(msg);
*/			}

		} catch (runtime_error e)
		{
			throw e;
		}

		if (Externalflag) {
			// get RAID info
			try
			{
				oam.getSystemConfig(systemConfig);
				RAIDDiskCritical = systemConfig.RAIDCriticalThreshold;
				RAIDDiskMajor = systemConfig.RAIDMajorThreshold;
				RAIDDiskMinor = systemConfig.RAIDMinorThreshold;

				//Log this event 
/*				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("RAID Config File System to monitor =");
				args.add(RAIDfs);
				msg.format(args);
				ml.logDebugMessage(msg);
*/
			} catch (runtime_error e)
			{
				throw e;
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

			long long totalBlocks;
			long long usedBlocks;

			if (!statvfs(fileName.c_str(), &buf)) {

				unsigned long blksize, blocks, freeblks, free; 
  
				blksize = buf.f_bsize; 
				blocks = buf.f_blocks; 
				freeblks = buf.f_bfree; 

				totalBlocks = blocks * blksize;
				free = freeblks * blksize; 
				usedBlocks = totalBlocks - free; 
			}
			else
				continue;

			double diskUsage = 0;
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

			if (diskUsage >= localDiskCritical && localDiskCritical > 0 ) {
				//adjust if over 100%
				if ( diskUsage > 100 )
					diskUsage = 100;
				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("Local Disk above Critical Disk threshold with a percentage of ");
				args.add((int) diskUsage);
				msg.format(args);
				ml.logInfoMessage(msg);
				serverMonitor.sendResourceAlarm(deviceName, DISK_USAGE_HIGH, SET, (int) diskUsage);
			}
			else if (diskUsage >= localDiskMajor && localDiskMajor > 0 ) {
				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("Local Disk above Major Disk threshold with a percentage of ");
				args.add((int) diskUsage);
				msg.format(args);
				ml.logInfoMessage(msg);
				serverMonitor.sendResourceAlarm(deviceName, DISK_USAGE_MED, SET, (int) diskUsage);
			}
			else if (diskUsage >= localDiskMinor && localDiskMinor > 0 ) {
				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("Local Disk above Minor Disk threshold with a percentage of ");
				args.add((int) diskUsage);
				msg.format(args);
				ml.logInfoMessage(msg);
				serverMonitor.sendResourceAlarm(deviceName, DISK_USAGE_LOW, SET, (int) diskUsage);
			}
			else
				serverMonitor.checkDiskAlarm(deviceName);
		}

		//check for external file systems/devices
		if (Externalflag) {
			for(int dbroot = 1; dbroot < (int) systemConfig.DBRootCount + 1 ; dbroot++)
			{
				string deviceName = systemConfig.DBRoot[dbroot-1];
				string fileName = deviceName + "/000.dir";
	
				long long totalBlocks;
				long long usedBlocks;
	
				if (!statvfs(fileName.c_str(), &buf)) {
	
					unsigned long blksize, blocks, freeblks, free; 
	
					blksize = buf.f_bsize; 
					blocks = buf.f_blocks; 
					freeblks = buf.f_bfree; 
	
					totalBlocks = blocks * blksize;
					free = freeblks * blksize; 
					usedBlocks = totalBlocks - free; 
				}
				else
					continue;
	
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
	
				if (diskUsage >= RAIDDiskCritical && RAIDDiskCritical > 0 ) {
					//adjust if over 100%
					if ( diskUsage > 100 )
						diskUsage = 100;
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
					serverMonitor.sendResourceAlarm(deviceName, DISK_USAGE_HIGH, SET, diskUsage);
				}
				else if (diskUsage >= RAIDDiskMajor && RAIDDiskMajor > 0 ) {
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
					serverMonitor.sendResourceAlarm(deviceName, DISK_USAGE_MED, SET, diskUsage);
				}
				else if (diskUsage >= RAIDDiskMinor && RAIDDiskMinor > 0 ) {
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
					serverMonitor.sendResourceAlarm(deviceName, DISK_USAGE_LOW, SET, diskUsage);
				}
				else
					serverMonitor.checkDiskAlarm(deviceName);
			}
		} // end of for loop

		//check OAM dbroot test flag to validate dbroot exist every MONITOR_PERIOD/6 if on pm
		for ( int timer = 0 ; timer < 60 ; timer++)
		{
			if ( moduleName.find("pm") != string::npos ) {
				//check OAM dbroot test flag to validate dbroot exist
				if ( dbrootList.size() != 0 ) {
					DBrootList::iterator p = dbrootList.begin();
					while ( p != dbrootList.end() )
					{
						string dbroot = (*p).dbrootDir;
				
						string fileName = dbroot + "/OAMdbrootCheck";
						// retry in case we hit the remount window
						for ( int retry = 0 ; ; retry++ )
						{
							ifstream file (fileName.c_str());
							if (!file) {
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
										args.add(dbroot);
										msg.format(args);
										ml.logWarningMessage(msg);

										string::size_type pos = dbroot.rfind("/",80);
										if (pos != string::npos)
											dbroot = dbroot.substr(pos+1,80);

										oam.sendDeviceNotification(dbroot, DBROOT_DOWN, moduleName);
										(*p).downFlag = true;
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
									args.add(dbroot);
									msg.format(args);
									ml.logWarningMessage(msg);
			
									string::size_type pos = dbroot.rfind("/",80);
									if (pos != string::npos)
										dbroot = dbroot.substr(pos+1,80);

									oam.sendDeviceNotification(dbroot, DBROOT_UP, moduleName);
									(*p).downFlag = false;
								}
								file.close();
								break;
							}
						}
						p++;
					}
				}
			}

			// sleep
			sleep(MONITOR_PERIOD/6);
		}

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
