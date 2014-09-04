/***************************************************************************
 * $Id: cpuMonitor.cpp 34 2006-09-29 21:13:54Z dhill $
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

float currentCpuUsage;
ProcessCPUList pcl;

pthread_mutex_t CPU_LOCK;

/**
 * constants define
 */ 

const std::string FE_MOUNT_DIR = "/var/log/Calpont/";	// FE mount dir
const int MONITOR_FREQ = 5;					// monitor frequency in sec
const int LOG_FREQ = 900;					// log frequency in sec
const int RESOURCE_DEBUG = false;
static unsigned int usage[LOG_FREQ/MONITOR_FREQ];
static int usageCount = 0;

/*****************************************************************************************
* @brief	cpuMonitor Thread
*
* purpose:	Get current CPU usage, average over 5 readings and report alarms
*
*****************************************************************************************/
void cpuMonitor()
{
	ServerMonitor serverMonitor;

	// register for Heartbeat monitoring
/*	try {
		ProcHeartbeat procheartbeat;
		procheartbeat.registerHeartbeat(CPU_HEARTBEAT_ID);
	}
	catch (exception& ex)
	{
		string error = ex.what();
		LoggingID lid(SERVER_MONITOR_LOG_ID);
		MessageLog ml(lid);
		Message msg;
		Message::Args args;
		args.add("EXCEPTION ERROR on registerHeartbeat: ");
		args.add(error);
		msg.format(args);
		ml.logErrorMessage(msg);
	}
	catch(...)
	{
		LoggingID lid(SERVER_MONITOR_LOG_ID);
		MessageLog ml(lid);
		Message msg;
		Message::Args args;
		args.add("EXCEPTION ERROR on sendHeartbeat: Caught unknown exception!");
		msg.format(args);
		ml.logErrorMessage(msg);
	}
*/
	int periodCount = 5;
	float cpuPeriod[periodCount];
	int periodCounter = 0;
	float averageCpuUsage = 0;
	currentCpuUsage = 0;

	// set defaults
	unsigned int cpuCritical = 0, 
				 cpuMajor = 0, 
				 cpuMinor = 0,
				 cpuMinorClear = 0;

	// initial cpu Period table
	for (int i =0;i < periodCount; i++)
	{
		cpuPeriod[i] = 0;
	}

	while(true)
	{
		// Get CPU usage water mark from server configuration and compare
		ModuleTypeConfig moduleTypeConfig;
		Oam oam;
		try {
			oam.getSystemConfig(moduleTypeConfig);
			cpuCritical = moduleTypeConfig.ModuleCPUCriticalThreshold; 
			cpuMajor = moduleTypeConfig.ModuleCPUMajorThreshold; 
			cpuMinor = moduleTypeConfig.ModuleCPUMinorThreshold;
			cpuMinorClear = moduleTypeConfig.ModuleCPUMinorClearThreshold;
		} catch (runtime_error e)
		{
			throw e;
		}

		if (RESOURCE_DEBUG)
			cout << "critical water: " << moduleTypeConfig.ModuleCPUCriticalThreshold << endl;

		pthread_mutex_lock(&CPU_LOCK);
		//
		// get Process and System CPU usage
		//
		serverMonitor.getCPUdata();

		// store and get average
		cpuPeriod[periodCounter] = currentCpuUsage;
		averageCpuUsage = 0;
		for (int i =0;i < periodCount; i++)
		{
			averageCpuUsage += cpuPeriod[i];
		}
		averageCpuUsage = averageCpuUsage / periodCount;

//		serverMonitor.logCPUactive(averageCpuUsage);
		if (CPU_DEBUG) {
			cout << "Current CPU Usage: " << currentCpuUsage << endl;
			cout << "Average CPU Usage: " << averageCpuUsage << endl;
		}

		if (averageCpuUsage >= cpuCritical && cpuCritical > 0 ) {
			serverMonitor.sendResourceAlarm("CPU", CPU_USAGE_HIGH, SET, (int) averageCpuUsage);
		}
		else if (averageCpuUsage >= cpuMajor && cpuMajor > 0 )
			serverMonitor.sendResourceAlarm("CPU", CPU_USAGE_MED, SET, (int) averageCpuUsage);
		else if (averageCpuUsage >= cpuMinor && cpuMinor > 0 )
			serverMonitor.sendResourceAlarm("CPU", CPU_USAGE_LOW, SET, (int) averageCpuUsage);
		else if (averageCpuUsage >= cpuMinorClear && cpuMinorClear > 0 ) {
			serverMonitor.checkCPUAlarm("CPU", CPU_USAGE_LOW);
			//Log this event 
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("Current CPU usage = ");
			args.add((int) currentCpuUsage);
			args.add(", Average CPU usage = ");
			args.add((int) averageCpuUsage);
			msg.format(args);
			ml.logInfoMessage(msg);
		}
		else
			serverMonitor.checkCPUAlarm("CPU");

		//
		// check CPU usage by process
		//
		ProcessCPUList::iterator p = pcl.begin();
		while(p != pcl.end())
		{
			string processName =  (*p).processName;
			double cpuUsage =  (*p).usedPercent;
			p++;

			if (CPU_DEBUG) {
				cout << "Process Name : " << processName << endl;
				cout << "CPU Usage: " << cpuUsage << endl;
			}

			// check if a Calpont Process, if so alarm is over thresholds
			// if not, just log if over thresholds
			if (cpuUsage >= cpuCritical && cpuCritical > 0) {
/*				try {
					t = oam.getMyProcessStatus(processID);
					processName = boost::get<1>(t);

					serverMonitor.sendResourceAlarm(processName, CPU_USAGE_HIGH, SET, (int) cpuUsage);
				}
				catch (...) {
*/						LoggingID lid(SERVER_MONITOR_LOG_ID);
						MessageLog ml(lid);
						Message msg;
						Message::Args args;
						args.add("Process");
						args.add(processName);
						args.add(" above Critical CPU threshold with a percentage of ");
						args.add((int) cpuUsage);
						msg.format(args);
						ml.logInfoMessage(msg);
//				}
			}
			else if (cpuUsage >= cpuMajor && cpuMajor > 0) {
/*				try {
					t = oam.getMyProcessStatus(processID);
					processName = boost::get<1>(t);

					serverMonitor.sendResourceAlarm(processName, CPU_USAGE_MED, SET, (int) cpuUsage);
				}
				catch (...) {
*/						LoggingID lid(SERVER_MONITOR_LOG_ID);
						MessageLog ml(lid);
						Message msg;
						Message::Args args;
						args.add("Process");
						args.add(processName);
						args.add(" above Major CPU threshold with a percentage of ");
						args.add((int) cpuUsage);
						msg.format(args);
						ml.logInfoMessage(msg);
//				}
			}
			else if (cpuUsage >= cpuMinor && cpuMinor > 0) {
/*				try {
					t = oam.getMyProcessStatus(processID);
					processName = boost::get<1>(t);

					serverMonitor.sendResourceAlarm(processName, CPU_USAGE_LOW, SET, (int) cpuUsage);
				}
				catch (...) {
*/						LoggingID lid(SERVER_MONITOR_LOG_ID);
						MessageLog ml(lid);
						Message msg;
						Message::Args args;
						args.add("Process");
						args.add(processName);
						args.add(" above Minor CPU threshold with a percentage of ");
						args.add((int) cpuUsage);
						msg.format(args);
						ml.logInfoMessage(msg);
//				}
			}
/*			else if (cpuUsage >= cpuMinorClear) {
				try {
					t = oam.getMyProcessStatus(processID);
					processName = boost::get<1>(t);

					serverMonitor.checkCPUAlarm(processName, CPU_USAGE_LOW);
				}
				catch (...) {}
			}
			else
				serverMonitor.checkCPUAlarm(processName);
*/		}

		// send heartbeat message
/*		try {
			ProcHeartbeat procheartbeat;
			procheartbeat.sendHeartbeat(CPU_HEARTBEAT_ID);

			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("Sent Heartbeat Msg");
			msg.format(args);
			ml.logInfoMessage(msg);
		}
		catch (exception& ex)
		{
			string error = ex.what();
			if ( error.find("Disabled") == string::npos ) {
				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("EXCEPTION ERROR on sendHeartbeat: ");
				args.add(error);
				msg.format(args);
				ml.logErrorMessage(msg);
			}
		}
		catch(...)
		{
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("EXCEPTION ERROR on sendHeartbeat: Caught unknown exception!");
			msg.format(args);
			ml.logErrorMessage(msg);
		}
*/

		pthread_mutex_unlock(&CPU_LOCK);

		// sleep
		sleep(MONITOR_PERIOD);

		++periodCounter;
		if ( periodCounter >= periodCount )
			periodCounter = 0;

	} // end of while loop
}

/******************************************************************************************
* @brief	checkCPUAlarm
*
* purpose:	check to see if an alarm(s) is set on CPU and clear if so
*
******************************************************************************************/
void ServerMonitor::checkCPUAlarm(string alarmItem, ALARMS alarmID)
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
			if ( oam.checkActiveAlarm(CPU_USAGE_HIGH, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, CPU_USAGE_HIGH);
			if ( oam.checkActiveAlarm(CPU_USAGE_MED, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, CPU_USAGE_MED);
			if ( oam.checkActiveAlarm(CPU_USAGE_LOW, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, CPU_USAGE_LOW);
			break;
		case CPU_USAGE_LOW: 	// clear high and medium alarms set if any found
			if ( oam.checkActiveAlarm(CPU_USAGE_HIGH, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, CPU_USAGE_HIGH);
			if ( oam.checkActiveAlarm(CPU_USAGE_MED, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, CPU_USAGE_MED);
			break;
		case CPU_USAGE_MED: 	// clear high alarms set if any found
			if ( oam.checkActiveAlarm(CPU_USAGE_HIGH, serverName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, CPU_USAGE_HIGH);
			break;
		default:			// none to clear
			break;
		} // end of switch
	return;
}

/*****************************************************************************************
* @brief	logCPUactive
*
* purpose:	Log Peak and Average CPU usage 
*
*****************************************************************************************/
void ServerMonitor::logCPUactive (unsigned int cpuUsage)
{
	ServerMonitor serverMonitor;

	// determin the active log file name
	string usageLogFileName = FE_MOUNT_DIR;
	usageLogFileName = usageLogFileName + "cpu.log";
	
	if (RESOURCE_DEBUG)
		cout << usageLogFileName << endl;
	
	fstream usageLogFile;
	usageLogFile.open (usageLogFileName.c_str(), ios::in|ios::out);

	if (usageLogFile.fail())
	{
		ofstream file (usageLogFileName.c_str());
		file.close();
		usageLogFile.open(usageLogFileName.c_str(), ios::in|ios::out);
		if (!usageLogFile) cout << "--error" << endl;
	}
	
	// get the counter
	usageLogFile.seekg(0, ios::beg);
	usageLogFile.read (reinterpret_cast<char *>(&usageCount), sizeof (int));
	if (usageLogFile.eof()) usageLogFile.clear();

	// new iteration
	if (usageCount == 0)
	{
		usageLogFile.seekp(0, ios::beg);
		usageLogFile.write (reinterpret_cast<char *>(&usageCount), sizeof (int));
	}
	usageCount ++;
	
	// append new usage data to the end
	usageLogFile.seekp (0, ios::end);
	usageLogFile.write (reinterpret_cast<char *>(&cpuUsage), sizeof (int));
	
	if (RESOURCE_DEBUG)
		cout << "usage: " << usageCount << endl;
	
	// calculate peak and average if it's time to log usage data
	if (usageCount >= LOG_FREQ / MONITOR_FREQ)
	{
		usageLogFile.seekg (4, ios::beg);
		usageLogFile.read ((char*)usage, sizeof(unsigned int) * LOG_FREQ/MONITOR_FREQ); 
		if (usageLogFile.eof()) usageLogFile.clear();
		if (RESOURCE_DEBUG)
		{
			for (int i = 0; i < usageCount; i++)
			{
				cout << usage [i] << endl;
			}
		}
		serverMonitor.logCPUstat(usageCount);
		
		// delete the file
		usageLogFile.close();
		unlink (usageLogFileName.c_str());
	}
	
	// else, update usageCount
	else
	{
		usageLogFile.seekp(0, ios::beg);
		usageLogFile.write (reinterpret_cast<char *>(&usageCount), sizeof (int));
		usageLogFile.close();
	}
}

/*****************************************************************************************
* @brief	logCPUstat
*
* purpose:	Log CPU stat using system API
*
*****************************************************************************************/
void ServerMonitor::logCPUstat (int usageCount)
{
	unsigned int max = 0;
	unsigned int sum = 0;
	float average = 0.0;
	for (int i = 0; i < usageCount; i++)
	{
		if (usage[i] > max)
			max = usage[i];
		sum += usage[i];
	}
	if ( usageCount == 0 )
		average=0;
	else
		average = sum / usageCount;
	
	// Call system log api to store stats.
	// for now, write on local for testing purpose.
	string statFileName = FE_MOUNT_DIR;
	statFileName = statFileName + "cpustat.log";
	ofstream file (statFileName.c_str(), ios::app);
	file << max << "	" << average << endl;
	file.close();
}

/*****************************************************************************************
* @brief	logCPUstat
*
* purpose:	Log CPU stat using system API
*
*****************************************************************************************/
void ServerMonitor::getCPUdata()
{
	pcl.clear();

	system("top -b -n1 | head -12 | awk '{print $9,$12}' | tail -5 > /tmp/infinidb_tmp_files/processCpu");

	ifstream oldFile1 ("/tmp/infinidb_tmp_files/processCpu");

	// read top 5 users
	int i = 0;
	char line[400];
	while (oldFile1.getline(line, 400))
	{
		string buf = line;
		string::size_type pos = buf.find (' ',0);
		if (pos != string::npos) {
			processCPU pc;
			pc.processName = buf.substr(pos+1,80);
			pc.usedPercent = atol(buf.substr(0,pos).c_str());
			pcl.push_back(pc);
			i++;
		}
	}
	oldFile1.close();

	//
	// get and check Total CPU usage
	//
	system("top -b -n 6 -d 1 | awk '{print $5}' | grep %id > /tmp/infinidb_tmp_files/systemCpu");

	ifstream oldFile ("/tmp/infinidb_tmp_files/systemCpu");

	float systemIdle = 0;
	// skip first line in file, and average the next 5 entries which contains idle times
	oldFile.getline(line, 400);
	int count = 0;
	while (oldFile.getline(line, 400))
	{
		string buf = line;
		string::size_type pos = buf.find ('%',0);
		if (pos != string::npos) {
			systemIdle = systemIdle + atol(buf.substr(0,pos-1).c_str());
			count++;
		}
	}
	oldFile.close();

	if ( count == 0 )
		currentCpuUsage = 0;
	else
		currentCpuUsage = 100 - (systemIdle / count);
}

