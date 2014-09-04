/***************************************************************************
 * $Id: resourceMonitor.cpp 1834 2011-02-08 14:37:11Z rdempsey $
 *
 *   Author: Zhixuan Zhu
 ***************************************************************************/
#include <iostream>
#include <fstream>
#include <iomanip>
#include <cstdlib>
#include <cerrno>
#include <exception>
#include <stdexcept>
#include "liboamcpp.h"

using namespace std;
using namespace oam;

/**
 * constants define
 */ 
enum HOST_INFO
{
	USER = 0,
	SYSTEM,
	NICE,
	IDLE
};

const string FE_MOUNT_DIR = "/var/log/Calpont/";	// FE mount dir
const int MONITOR_FREQ = 15;				// monitor frequency in sec
const int LOG_FREQ = 900;					// log frequency in sec
const int DEBUG = false;

/**
 * global variables
 */
static char cpu[5];
static unsigned int usage[LOG_FREQ/MONITOR_FREQ];
static int usageCount = 0;

/**
 * @brief get cpu usage for cpuNo
 */
unsigned int *getUsage(char* cpuNo);

/**
 * @brief get cpu usage diffence over 3 seconds
 */
unsigned int *diffUsage();

/**
 * @brief calculate overall cpu usage
 */
unsigned int calcCPUusage(unsigned int *times);

/**
 * @brief log cpu usage to active log file
 */
void logCPUactive (unsigned int); 

/**
 * @brief log cpu peak and average to stat file
 */
void logCPUstat ();


/*****************************************************************************************
* @brief	main function
*
* purpose:	Get current CPU usage and return current state
*
*****************************************************************************************/
int main (int argc, char** argv)
{
	unsigned int *times;
	unsigned int cpuUsage = 0;
	// set defaults
	unsigned int cpuCritical = 9000, 
				 cpuMajor = 8000, 
				 cpuMinor = 7000,
				 cpuMinorClear = 6000;
	
	strcpy (cpu, "cpu");
		
	// get specific CPU info
	times = diffUsage();
	
	// no cpus available on the system
	if (times == NULL)
	{
		cout << "No cpus on the system" << endl;
		return 0;
	}

	cpuUsage = calcCPUusage(times);
	logCPUactive(cpuUsage);
	cout << "cpuUsage: " << cpuUsage << endl;
		
	// Get CPU usage water mark from server configuration and compare
	ServerConfig serverConfig;
	Oam oam;
	
	try {
		oam.getSystemConfig (serverConfig);
		cpuCritical = serverConfig.ServerCPUCriticalThreshold; 
		cpuMajor = serverConfig.ServerCPUMajorThreshold; 
		cpuMinor = serverConfig.ServerCPUMinorThreshold;
		cpuMinorClear = serverConfig.ServerCPUMinorClearThreshold;
	} catch (runtime_error e)
	{
		throw e;
	}
	cout << "critical water: " << serverConfig.ServerCPUCriticalThreshold << endl;
	
	if (cpuUsage >= cpuCritical)
	{
		cout << "return critical: " << CRITICAL << endl;
		return CRITICAL;
	}
	else if (cpuUsage >= cpuMajor)
	{
		cout << "return major: " << MAJOR << endl;
		return MAJOR;
	}
	else if (cpuUsage >= cpuMinor)
	{
		cout << "return Minor: " << MINOR << endl;
		return MINOR;
	}
	else if (cpuUsage >= cpuMinorClear)
	{
		cout << "return MinorClear: " << WARNING << endl;
		return WARNING;
	}
	else
	{
		cout << "return Below MinorClear: " << NO_SEVERITY << endl;
		return NO_SEVERITY;
	}
}
	
/*****************************************************************************************
* @brief	diffUsage
*
* purpose:	Compare usage different for changes
*
*****************************************************************************************/
unsigned int *diffUsage() 
{
	static unsigned int times1[4];
	unsigned int *times;
	int i;
	
	times = getUsage(cpu);
	if (times == NULL)
		return NULL;
	memcpy(times1, getUsage(cpu), sizeof(unsigned int) * 4);
	sleep(3);
	times = getUsage(cpu);

	for(i = 0; i < 4; i++) 
		times1[i] = times[i] - times1[i];
	return times1;
}

/*****************************************************************************************
* @brief	diffUsage
*
* purpose:	Compare usage different for changes
*
*****************************************************************************************/
unsigned int *getUsage(char* cpuNo) 
{
	static unsigned int times[4];
	char tmp[5];
	char str[80];
	FILE *file;

	file = fopen("/proc/stat", "r");
	while (fgets(str, 80, file))
	{
		// search for cpuNo
		if ((strstr (str, cpuNo) != NULL))
		{
			sscanf(str, "%s %u %u %u %u", tmp, 
			       &times[0], &times[1], &times[2], &times[3]);
			fclose(file);
			return times;
		}
	}
	fclose(file);
	return NULL;
}

/*****************************************************************************************
* @brief	calcCPUusage
*
* purpose:	Calculate CPU usage
*
*****************************************************************************************/
unsigned int calcCPUusage (unsigned int *times)
{
	unsigned int total = 0;
	
	for(int i = 0; i < 4; i++)
		total += times[i];

	double load = (double)times[IDLE] * 100.0 / (double)total;
	return (int)((100.0-load));	
}

/*****************************************************************************************
* @brief	logCPUactive
*
* purpose:	Log Peak and Average CPU usage 
*
*****************************************************************************************/
void logCPUactive (unsigned int cpuUsage)
{
	// determin the active log file name
	string usageLogFileName = FE_MOUNT_DIR;
	usageLogFileName = usageLogFileName + cpu + ".log";
	
	if (DEBUG)
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
	
	if (DEBUG)
		cout << "usage: " << usageCount << endl;
	
	// calculate peak and average if it's time to log usage data
	if (usageCount >= LOG_FREQ / MONITOR_FREQ)
	{
		usageLogFile.seekg (4, ios::beg);
		usageLogFile.read ((char*)usage, sizeof(unsigned int) * LOG_FREQ/MONITOR_FREQ); 
		if (usageLogFile.eof()) usageLogFile.clear();
		if (DEBUG)
		{
			for (int i = 0; i < usageCount; i++)
			{
				cout << usage [i] << endl;
			}
		}
		logCPUstat();
		
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
void logCPUstat ()
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
	average = sum / usageCount;
	
	// Call system log api to store stats.
	// for now, write on local for testing purpose.
	string statFileName = FE_MOUNT_DIR;
	statFileName = statFileName + cpu + "stat.log";
	ofstream file (statFileName.c_str(), ios::app);
	file << max << "	" << average << endl;
	file.close();
}
