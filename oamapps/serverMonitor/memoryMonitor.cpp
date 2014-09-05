/***************************************************************************
 * $Id: memoryMonitor.cpp 34 2006-09-29 21:13:54Z dhill $
 *
 *   Author: Zhixuan Zhu
 ***************************************************************************/

#include "serverMonitor.h"

using namespace std;
using namespace oam;
using namespace snmpmanager;
using namespace logging;
using namespace servermonitor;
//using namespace procheartbeat;

unsigned long totalMem;
ProcessMemoryList pml;

pthread_mutex_t MEMORY_LOCK;

/*****************************************************************************************
* @brief	memoryMonitor Thread
*
* purpose:	Get current Memory and Swap usage, report alarms
*
*****************************************************************************************/
void memoryMonitor()
{
	ServerMonitor serverMonitor;

	int swapUsagePercent = 0;

  	struct sysinfo myinfo; 

	// set defaults
	int memoryCritical = 90, 
				 memoryMajor = 0, 
				 memoryMinor = 0,
				 swapCritical = 90, 
				 swapMajor = 80, 
				 swapMinor = 70;

	int day = 0;

	//set monitoring period to 60 seconds
	int monitorPeriod = MONITOR_PERIOD;

	while(true)
	{
		// Get MEMORY usage water mark from server configuration and compare
		ModuleTypeConfig moduleTypeConfig;
		Oam oam;
		
		try {
			oam.getSystemConfig (moduleTypeConfig);
			memoryCritical = moduleTypeConfig.ModuleMemCriticalThreshold; 
			memoryMajor = moduleTypeConfig.ModuleMemMajorThreshold; 
			memoryMinor = moduleTypeConfig.ModuleMemMinorThreshold;
			swapCritical = moduleTypeConfig.ModuleSwapCriticalThreshold; 
			swapMajor = moduleTypeConfig.ModuleSwapMajorThreshold; 
			swapMinor = moduleTypeConfig.ModuleSwapMinorThreshold;
		} catch (...)
		{
			sleep(5);
			continue;
		}
		
		// get cache MEMORY stats
		system("cat /proc/meminfo | grep Cached -m 1 | awk '{print $2}' > /tmp/cached");

		ifstream oldFile ("/tmp/cached");

		string strCache;
		long long cache;

		char line[400];
		while (oldFile.getline(line, 400))
		{
			strCache = line;
			break;
		}
		oldFile.close();

		if (strCache.empty() )
			cache = 0;
		else
			cache = atol(strCache.c_str()) * 1024;

		try{
	  		sysinfo(&myinfo);
		}
		catch (...) {}

		//get memory stats
		totalMem = myinfo.totalram ; 
		unsigned long freeMem = myinfo.freeram ;

		// adjust for cache, which is available memory
		unsigned long usedMem = totalMem - freeMem - cache;

		//get swap stats
		unsigned long totalSwap = myinfo.totalswap ;
		unsigned long freeswap = myinfo.freeswap ;
		unsigned long usedSwap = totalSwap - freeswap ;

		if ( totalSwap == 0 ) {
			swapUsagePercent = 0;

			//get current day, log warning only once a day
			time_t now;
			now = time(NULL);
			struct tm tm;
			localtime_r(&now, &tm);

			if ( day != tm.tm_mday) {
				day = tm.tm_mday;

				//Log this event 
				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("Total Swap space is set to 0");
				msg.format(args);
				ml.logWarningMessage(msg);
			}
		}
		else
			swapUsagePercent =  usedSwap / (totalSwap / 100);

		int memoryUsagePercent;
		if ( totalMem == 0 ) {
			memoryUsagePercent = 0;

			//Log this event 
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("Total Memory space is set to 0");
			msg.format(args);
			ml.logWarningMessage(msg);
		}
		else
			memoryUsagePercent =  (usedMem / (totalMem / 100)) + 1;

		// check for Memory alarms
		if (memoryUsagePercent >= memoryCritical && memoryCritical > 0 ) {
			if ( monitorPeriod == MONITOR_PERIOD ) {
				//first time called, log
				//adjust if over 100%
				if ( memoryUsagePercent > 100 )
					memoryUsagePercent = 100;

				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("Local Memory above Critical Memory threshold with a percentage of ");
				args.add((int) memoryUsagePercent);
				args.add(" ; Swap ");
				args.add((int) swapUsagePercent);
				msg.format(args);
				ml.logInfoMessage(msg);
				serverMonitor.sendResourceAlarm("Local-Memory", MEMORY_USAGE_HIGH, SET, memoryUsagePercent);

				pthread_mutex_lock(&MEMORY_LOCK);
				serverMonitor.outputProcMemory(true);
				pthread_mutex_unlock(&MEMORY_LOCK);
			}
			// change to 1 second for quick swap space monitoring
			monitorPeriod = 1;
		}
		else if (memoryUsagePercent >= memoryMajor && memoryMajor > 0 ) {
			monitorPeriod = MONITOR_PERIOD;
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("Local Memory above Major Memory threshold with a percentage of ");
			args.add((int) memoryUsagePercent);
			msg.format(args);
			ml.logInfoMessage(msg);
			serverMonitor.sendResourceAlarm("Local-Memory", MEMORY_USAGE_MED, SET, memoryUsagePercent);
		}
		else if (memoryUsagePercent >= memoryMinor && memoryMinor > 0 ) {
			monitorPeriod = MONITOR_PERIOD;
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("Local Memory above Minor Memory threshold with a percentage of ");
			args.add((int) memoryUsagePercent);
			msg.format(args);
			ml.logInfoMessage(msg);
			serverMonitor.sendResourceAlarm("Local-Memory", MEMORY_USAGE_LOW, SET, memoryUsagePercent);
		}
		else {
			monitorPeriod = MONITOR_PERIOD;
			serverMonitor.checkMemoryAlarm("Local-Memory");
		}

		// check for Swap alarms
		if (swapUsagePercent >= swapCritical && swapCritical > 0 ) {
			//adjust if over 100%
			if ( swapUsagePercent > 100 )
				swapUsagePercent = 100;
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("Swap above Critical Memory threshold with a percentage of ");
			args.add((int) swapUsagePercent);
			msg.format(args);
			ml.logInfoMessage(msg);
			serverMonitor.sendResourceAlarm("Swap", SWAP_USAGE_HIGH, SET, swapUsagePercent);
			serverMonitor.checkSwapAction();
		}
		else if (swapUsagePercent >= swapMajor && swapMajor > 0 ) {
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("Swap above Major Memory threshold with a percentage of ");
			args.add((int) swapUsagePercent);
			msg.format(args);
			ml.logInfoMessage(msg);
			serverMonitor.sendResourceAlarm("Swap", SWAP_USAGE_MED, SET, swapUsagePercent);
			serverMonitor.checkSwapAction();
		}
		else if (swapUsagePercent >= swapMinor && swapMinor > 0 ) {
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("Swap above Minor Memory threshold with a percentage of ");
			args.add((int) swapUsagePercent);
			msg.format(args);
			ml.logInfoMessage(msg);
			serverMonitor.sendResourceAlarm("Swap", SWAP_USAGE_LOW, SET, swapUsagePercent);
		}
		else
			serverMonitor.checkSwapAlarm("Swap");

		// sleep, 1 minute
		sleep(monitorPeriod);

	} // end of while loop
}

/******************************************************************************************
* @brief	checkMemoryAlarm
*
* purpose:	check to see if an alarm(s) is set on MEMORY and clear if so
*
******************************************************************************************/
void ServerMonitor::checkMemoryAlarm(string alarmItem, ALARMS alarmID)
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
			if ( oam.checkActiveAlarm(MEMORY_USAGE_HIGH, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, MEMORY_USAGE_HIGH);
			if ( oam.checkActiveAlarm(MEMORY_USAGE_MED, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, MEMORY_USAGE_MED);
			if ( oam.checkActiveAlarm(MEMORY_USAGE_LOW, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, MEMORY_USAGE_LOW);
			break;
		case MEMORY_USAGE_LOW: 	// clear high and medium alarms set if any found
			if ( oam.checkActiveAlarm(MEMORY_USAGE_HIGH, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, MEMORY_USAGE_HIGH);
			if ( oam.checkActiveAlarm(MEMORY_USAGE_MED, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, MEMORY_USAGE_MED);
			break;
		case MEMORY_USAGE_MED: 	// clear high alarms set if any found
			if ( oam.checkActiveAlarm(MEMORY_USAGE_HIGH, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, MEMORY_USAGE_HIGH);
			break;
		default:			// none to clear
			break;
		} // end of switch
	return;
}

/******************************************************************************************
* @brief	checkSwapAlarm
*
* purpose:	check to see if an alarm(s) is set on SWAP and clear if so
*
******************************************************************************************/
void ServerMonitor::checkSwapAlarm(string alarmItem, ALARMS alarmID)
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
			if ( oam.checkActiveAlarm(SWAP_USAGE_HIGH, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, SWAP_USAGE_HIGH);
			if ( oam.checkActiveAlarm(SWAP_USAGE_MED, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, SWAP_USAGE_MED);
			if ( oam.checkActiveAlarm(SWAP_USAGE_LOW, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, SWAP_USAGE_LOW);
			break;
		case SWAP_USAGE_LOW: 	// clear high and medium alarms set if any found
			if ( oam.checkActiveAlarm(SWAP_USAGE_HIGH, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, SWAP_USAGE_HIGH);
			if ( oam.checkActiveAlarm(SWAP_USAGE_MED, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, SWAP_USAGE_MED);
			break;
		case SWAP_USAGE_MED: 	// clear high alarms set if any found
			if ( oam.checkActiveAlarm(SWAP_USAGE_HIGH, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, SWAP_USAGE_HIGH);
			break;
		default:			// none to clear
			break;
		} // end of switch
	return;
}

/******************************************************************************************
* @brief	checkSwapAction
*
* purpose:	check if any system action needs tyo be taken
*
******************************************************************************************/
void ServerMonitor::checkSwapAction()
{
	Oam oam;
	string swapAction = "restartSystem";
	
	try {
		oam.getSystemConfig ("SwapAction", swapAction);
	} catch (...)
	{}

	if (swapAction == "none")
		return;

	LoggingID lid(SERVER_MONITOR_LOG_ID);
	MessageLog ml(lid);
	Message msg;
	Message::Args args;
	args.add("Swap Space usage over Major threashold, perform OAM command ");
	args.add( swapAction);
	msg.format(args);
	ml.logCriticalMessage(msg);

	GRACEFUL_FLAG gracefulTemp = GRACEFUL;
	ACK_FLAG ackTemp = ACK_YES;

	if ( swapAction == "stopSystem") {
		try
		{
			oam.stopSystem(gracefulTemp, ackTemp);
		}
		catch (exception& e)
		{
		}
	}
	else if ( swapAction == "restartSystem") {
		try
		{
			oam.restartSystem(gracefulTemp, ackTemp);
		}
		catch (exception& e)
		{
		}
	}
}

/******************************************************************************************
* @brief	outputProcMemory
*
* purpose:	output Top memory users
*
******************************************************************************************/
void ServerMonitor::outputProcMemory(bool log)
{
	//
	// get top 5 Memory users by process
	//

	system("ps -e -orss=1,args= | sort -b -k1,1n |tail -n 5 | awk '{print $1,$2}' > /tmp/infinidb_tmp_files/processMem");

	ifstream oldFile ("/tmp/infinidb_tmp_files/processMem");

	string process;
	long long memory;
	int memoryUsage;
	pml.clear();

	char line[400];
	while (oldFile.getline(line, 400))
	{
		string buf = line;
		string::size_type pos = buf.find (' ',0);
		if (pos != string::npos) {
			memory = atol(buf.substr(0,pos-1).c_str());
			memoryUsage = (memory * 1024 * 1000 / totalMem) + 1 ;
			process = buf.substr(pos+1,80);

			//cleanup process name
			pos = process.rfind ('/');
			if (pos != string::npos)
				process=process.substr(pos+1,80);
			else
			{
				pos = process.find ('[',0);
				if (pos != string::npos)
					process=process.substr(pos+1,80);
				pos = process.find (']',0);
				if (pos != string::npos)
					process=process.substr(0,pos);
			}

			processMemory pm;
			pm.processName = process;
			pm.usedBlocks = memory;
			pm.usedPercent = memoryUsage;
			pml.push_back(pm);

			if (log) {
				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("Memory Usage for Process: ");
				args.add(process);
				args.add(" : Memory Used ");
				args.add((int) memory);
				args.add(" : % Used ");
				args.add(memoryUsage);
				msg.format(args);
				ml.logInfoMessage(msg);
			}
		}
	}
	oldFile.close();
}
