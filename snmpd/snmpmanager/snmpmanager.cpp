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
* Author: Zhixuan Zhu
******************************************************************************************/
#define SNMPMANAGER_DLLEXPORT
#include "snmpmanager.h"
#undef SNMPMANAGER_DLLEXPORT

#include <unistd.h>
#include <cstdio>
#include <algorithm>
#include <vector>
#include <iterator>

#include "messagequeue.h"
#include "snmpglobal.h"
#include "liboamcpp.h"
#include "installdir.h"

using namespace std;
using namespace oam;
using namespace messageqcpp;
using namespace logging;

namespace snmpmanager {

#ifdef __linux__
inline pid_t gettid(void) { return syscall(__NR_gettid); }
#else
inline pid_t gettid(void) { return getpid(); }
#endif

/*****************************************************************************************
* @brief	Constructor
*
* purpose:
*
*****************************************************************************************/

SNMPManager::SNMPManager()
{
	Oam oam;
	// Get Parent OAM Module Name
	try{
		oam.getSystemConfig("ParentOAMModuleName", SNMPManager::parentOAMModuleName);
	}
	catch(...)
	{
		//Log event
		LoggingID lid(11);
		MessageLog ml(lid);
		Message msg;
		Message::Args args;
		args.add("Failed to read Parent OAM Module Name");
		msg.format(args);
		ml.logErrorMessage(msg);
     	throw runtime_error ("Failed to read Parent OAM Module Name");
	}

}

/*****************************************************************************************
* @brief	Destructor
*
* purpose:
*
*****************************************************************************************/

SNMPManager::~SNMPManager()
{
}

/*****************************************************************************************
* @brief	sendAlarmReport API
*
* purpose:	Process Alarm Report
*
*****************************************************************************************/
void SNMPManager::sendAlarmReport (const char* componentID, int alarmID, int state, 
									std::string repModuleName, std::string repProcessName)
{

#ifdef SKIP_SNMP
	return;
#else
	LoggingID lid(11);
	MessageLog ml(lid);
	Message msg;
	Message::Args args;

	//Log receiving of Alarm report
	if (CALPONT_SNMP_DEBUG)
	{
		args.add("sendAlarmReport: alarm #");
		args.add(alarmID);
		args.add(", state: ");
		args.add(state);
		args.add(", component: ");
		args.add(componentID);
		msg.format(args);
		ml.logDebugMessage(msg);
	}

	Oam oam;

	// get current Module name
	string ModuleName;
	if ( repModuleName.empty()) {
		oamModuleInfo_t st;
		try {
			st = oam.getModuleInfo();
			ModuleName = boost::get<0>(st);
		}
		catch (...) {
			ModuleName = "Unknown Reporting Module";
		}
	}
	else
		ModuleName = repModuleName;

	//
	// FILTERING: Don't process CLEAR alarms if there is no associated SET alarm
	//
// comment out, issues with race conditions when coded here..
/*
	if (state == CLEAR) {
		// get active alarms
		AlarmList alarmList;
		SNMPManager sm;
		sm.getActiveAlarm (alarmList);

		bool found = false;
		AlarmList::iterator i;
		for (i = alarmList.begin(); i != alarmList.end(); ++i)
		{
			// check if matching ID
			if (alarmID != (i->second).getAlarmID() )
				continue;

			string ScomponentID = componentID;
			// check if the same fault component on same Module
			if (ScomponentID.compare((i->second).getComponentID()) == 0 &&
				ModuleName.compare((i->second).getSname()) == 0) {
				found = true;
				break;
			}
		}
		// check is a SET alarm was found, if not return
		if (!found)
			return;
	}
*/
	// get pid, tid info
	int pid = getpid();	
	int tid = gettid();	

	// get reporting Pprocess Name
	string processName;
	if ( repProcessName.empty()) {
		// get current process name
		myProcessStatus_t t;
		try {
			t = oam.getMyProcessStatus();
			processName = boost::get<1>(t);
		}
		catch (...) {
			processName = "Unknown-Reporting-Process";
		}
	}
	else
		processName = repProcessName;

	string ComponentID = componentID;
	// send Trap

	string cmd = startup::StartUp::installDir() + "/bin/sendtrap " + ComponentID + " " + oam.itoa(alarmID) + " " + oam.itoa(state) + " " + ModuleName + " " + processName + " " + oam.itoa(pid) + " " + oam.itoa(tid) + " " + SNMPManager::parentOAMModuleName;

	system(cmd.c_str());

	return;
#endif //SKIP_SNMP
}

/*****************************************************************************************
* @brief	getActiveAlarm API
*
* purpose:	Get List of Active Alarm from activealarm file
*
*****************************************************************************************/
void SNMPManager::getActiveAlarm(AlarmList& alarmList) const
{
	//add-on to fileName with mount name if on non Parent Module
	Oam oam;
	string fileName = ACTIVE_ALARM_FILE;

   	int fd = open(fileName.c_str(),O_RDONLY);

   	if (fd == -1) {
     	// file may being deleted temporarily by trapHandler
	 	sleep (1);
   		fd = open(fileName.c_str(),O_RDONLY);
   		if (fd == -1) {
			// no active alarms, return
			return;
	 	}
	}

	ifstream activeAlarm (fileName.c_str(), ios::in);

    // acquire read lock
	if (flock(fd,LOCK_SH) == -1) 
	{
     	throw runtime_error ("Lock active alarm log file error");
	}
	
	Alarm alarm;
	
	while (!activeAlarm.eof())
	{
		activeAlarm >> alarm;
		if (alarm.getAlarmID() != INVALID_ALARM_ID)
			//don't sort
//			alarmList.insert (AlarmList::value_type(alarm.getAlarmID(), alarm));
			alarmList.insert (AlarmList::value_type(INVALID_ALARM_ID, alarm));
	}
	activeAlarm.close();
	
	// release lock
	if (flock(fd,LOCK_UN) == -1) 
	{
     	throw runtime_error ("Release lock active alarm log file error");
	}

	close(fd);

	if (CALPONT_SNMP_DEBUG)
	{
		AlarmList :: iterator i;
		for (i = alarmList.begin(); i != alarmList.end(); ++i)
		{
			cout << i->second << endl;
		}
	}
	return;
}

/*****************************************************************************************
* @brief	getAlarm API
*
* purpose:	Get List of Historical Alarms from alarm file
*
*			date = MM/DD/YY format
*
*****************************************************************************************/
void SNMPManager::getAlarm(std::string date, AlarmList& alarmList) const
{
	string alarmFile = "/tmp/alarms";

	//make 1 alarm log file made up of archive and current alarm.log
	(void)system("touch /tmp/alarms");

	string cmd = ("ls " + ALARM_ARCHIVE_FILE + " | grep 'alarm.log' > /tmp/alarmlogfiles");
	(void)system(cmd.c_str());

	string fileName = "/tmp/alarmlogfiles";

	ifstream oldFile (fileName.c_str());
	if (oldFile) {
		char line[200];
		string buf;
		while (oldFile.getline(line, 200))
		{
			buf = line;
			string cmd = "cat " + ALARM_ARCHIVE_FILE + "/" + buf + " >> /tmp/alarms";
			(void)system(cmd.c_str());
		}
	
		oldFile.close();
		unlink (fileName.c_str());
	}

	cmd = "cat " + ALARM_FILE + " >> /tmp/alarms";
	(void)system(cmd.c_str());

   	int fd = open(alarmFile.c_str(),O_RDONLY);
	
   	if (fd == -1)
 		// doesn't exist yet, return
		return;

	ifstream hisAlarm (alarmFile.c_str(), ios::in);

	// acquire read lock
	if (flock(fd,LOCK_SH) == -1) 
	{
     	throw runtime_error ("Lock alarm log file error");
	}

	//get mm / dd / yy from incoming date
	string mm = date.substr(0,2);
	string dd = date.substr(3,2);
	string yy = date.substr(6,2);

	Alarm alarm;
	
	while (!hisAlarm.eof())
	{
		hisAlarm >> alarm;
		if (alarm.getAlarmID() != INVALID_ALARM_ID) {
			time_t cal = alarm.getTimestampSeconds();
			struct tm tm;
			localtime_r(&cal, &tm);
			char tmp[3];
			strftime (tmp, 3, "%m", &tm);
			string alarm_mm = tmp;
			alarm_mm = alarm_mm.substr(0,2);
			strftime (tmp, 3, "%d", &tm);
			string alarm_dd = tmp;
			alarm_dd = alarm_dd.substr(0,2);
			strftime (tmp, 3, "%y", &tm);
			string alarm_yy = tmp;
			alarm_yy = alarm_yy.substr(0,2);

			if ( mm == alarm_mm && dd == alarm_dd && yy == alarm_yy )
				//don't sort
	//			alarmList.insert (AlarmList::value_type(alarm.getAlarmID(), alarm));
				alarmList.insert (AlarmList::value_type(INVALID_ALARM_ID, alarm));
		}
	}
	hisAlarm.close();
	unlink (alarmFile.c_str());
	
	// release lock
	if (flock(fd,LOCK_UN) == -1) 
	{
     	throw runtime_error ("Release lock active alarm log file error");
	}
	
	if (CALPONT_SNMP_DEBUG)
	{
		AlarmList :: iterator i;
		for (i = alarmList.begin(); i != alarmList.end(); ++i)
		{
			cout << i->second << endl;
		}
	}
}

/*****************************************************************************************
* @brief	getNMSAddr API
*
* purpose:	Get NMS IP Address from the snmptrapd config file
*
*****************************************************************************************/
void SNMPManager::getNMSAddr (string& addr)
{
	getSNMPConfig (SNMPManager::parentOAMModuleName, TRAPD, "NMSADDR", addr);
}

/*****************************************************************************************
* @brief	setNMSAddr API
*
* purpose:	Set NMS IP Address from the snmptrapd config file
*
*****************************************************************************************/
void SNMPManager::setNMSAddr (const string addr)
{
	setSNMPConfig (SNMPManager::parentOAMModuleName, TRAPD, "NMSADDR", addr);
}


/*****************************************************************************************
* @brief	setSNMPConfig API
*
* purpose:	Set a SNMP monitoring threashold value in the snmpdx config file
*			Set NMS IP Address in snmptrapd config file
*
*
*****************************************************************************************/
void SNMPManager::setSNMPConfig (const string ModuleName, const std::string agentName,
									const string paramName, const string value)
{
	string fileName;
	makeFileName (agentName, fileName);
	vector <string> lines;
	string calParamName = "CALPONT_" + paramName;
	Oam oam;

	ifstream oldFile (fileName.c_str());
	if (!oldFile) throw runtime_error ("No configuration file found");
	
	char line[200];
	string buf;
	string changeValue;
	string delimiters = " ";
	while (oldFile.getline(line, 200))
	{
		buf = line;
		if (buf.find(calParamName) != string::npos)
		{
			lines.push_back(buf);
			// change next line
			oldFile.getline(line, 200);
			buf = line;

			// if change NMS IP Addr to 0.0.0.0, then comment out forward line if not already
			if ( paramName == "NMSADDR" && value == oam::UnassignedIpAddr ) {
				string::size_type pos = buf.find("#", 0);
				if ( string::npos == pos ) {
					string templine = line;
					buf = "#" + templine;
				}
			}

			// if change NMS IP Addr to  NOT 0.0.0.0, then un-comment line if needed
			if ( paramName == "NMSADDR" && value != oam::UnassignedIpAddr ) {
				string::size_type pos = buf.find("#", 0);
				if ( string::npos != pos )
					buf = buf.substr(1,200);
			}

			string::size_type lastPos = buf.find_first_not_of(delimiters, 0);
    		string::size_type pos     = buf.find_first_of(delimiters, lastPos);

    		// find the last token, which is the value to be changed
			while (string::npos != pos || string::npos != lastPos)
    		{
        		changeValue = buf.substr(lastPos, pos - lastPos);
        		lastPos = buf.find_first_not_of(delimiters, pos);
           		pos = buf.find_first_of(delimiters, lastPos);
    		}
    		pos = buf.find(changeValue);
    		buf.replace(pos, 20, value);
		}
		
		// output to temp file
		lines.push_back(buf);
	}
	
	oldFile.close();
	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());	
	
	// create new file
	int fd = open(fileName.c_str(),O_RDWR|O_CREAT, 0666);
	
	// Aquire an exclusive lock
   	if (flock(fd,LOCK_EX) == -1) {
    	throw runtime_error ("Lock SNMP configuration file error");
   	}

	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	// Release lock
	if (flock(fd,LOCK_UN) == -1)
	{
    	throw runtime_error ("Release lock SNMP configuration file error");		
	}
	close(fd);

	//re-init snmp processes
	string processName;
	makeProcessName (agentName, processName);

/*	try {
		oam.reinitProcessType(processName);
	}
	catch(...)
	{}
*/
}

/*****************************************************************************************
* @brief	getSNMPConfig API
*
* purpose:	Get a SNMP monitoring threashold value in the snmpdx config file
*
*			paramName options: DISK_CRITICAL, DISK_MAJOR, DISK_MINOR
*								MEM_CRITICAL, MEM_MAJOR, MEM_MINOR
*								SWAP_CRITICAL, SWAP_MAJOR, SWAP_MINOR
*
*****************************************************************************************/
void SNMPManager::getSNMPConfig (const string ModuleName, const std::string agentName,
									const string paramName, string& value)
{
	string fileName;
	makeFileName (agentName, fileName);

	ifstream configFile (fileName.c_str());
	char line[200];
	string buf;
	string delimiters = " ";
	string calParamName = "CALPONT_" + paramName;

	while (configFile.getline(line, 200))
	{
		buf = line;
		if (buf.find(calParamName) != string::npos)
		{
			configFile.getline(line, 200);
			buf = line;
			string::size_type lastPos = buf.find_first_not_of(delimiters, 0);
    		string::size_type pos     = buf.find_first_of(delimiters, lastPos);

    		// find the last token, which is the value to be read
			while (string::npos != pos || string::npos != lastPos)
    		{
        		value = buf.substr(lastPos, pos - lastPos);
        		lastPos = buf.find_first_not_of(delimiters, pos);
        		pos = buf.find_first_of(delimiters, lastPos);
    		}
    		return;
    	}
	}
	throw runtime_error("Error processing snmptrapd configuration file");
}

/*****************************************************************************************
* @brief	setSNMPModuleName API
*
* purpose:	Set SNMP Module name in the snmpdx.conf file
*
*****************************************************************************************/
void SNMPManager::setSNMPModuleName ()
{
	// get current Module name
	Oam oam;
	string ModuleName;
	oamModuleInfo_t st;
	try {
		st = oam.getModuleInfo();
		ModuleName = boost::get<0>(st);
	}
	catch (...) {
		ModuleName = "Unknown Report Module";
	}

	string agentName = SUB_AGENT;
	string fileName;
	makeFileName (agentName, fileName);
	vector <string> lines;

	ifstream oldFile (fileName.c_str());
	if (!oldFile) throw runtime_error ("No configuration file found");
	
	char line[200];
	string buf;
	string newLine;
	string newLine1;
	string delimiters = " ";
	while (oldFile.getline(line, 200))
	{
		buf = line;
		string::size_type pos = buf.find("ModuleNameStub",0);
		if (pos != string::npos)
		{
	        newLine = buf.substr(0, pos);
    	    newLine.append(ModuleName);

			string::size_type pos1 = buf.find("|",pos);
			if (pos1 != string::npos)
			{
	        	newLine1 = buf.substr(pos1, 200);
    	    	newLine.append(newLine1);
			}
		buf = newLine;
		}
		// output to temp file
		lines.push_back(buf);
	}
	
	oldFile.close();
	unlink (fileName.c_str());
   	ofstream newFile (fileName.c_str());	
	
	// create new file
	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	
	// Aquire an exclusive lock
   	if (flock(fd,LOCK_EX) == -1) {
    	throw runtime_error ("Lock SNMP configuration file error");
   	}

	copy(lines.begin(), lines.end(), ostream_iterator<string>(newFile, "\n"));
	newFile.close();
	
	// Release lock
	if (flock(fd,LOCK_UN) == -1)
	{
    	throw runtime_error ("Release lock SNMP configuration file error");		
	}
	close(fd);
}


/**
 * @brief Private functions definition
 */

/*****************************************************************************************
* @brief	makeFileName API
*
* purpose:	returns path for associated snmp conf file
*
*****************************************************************************************/
void SNMPManager::makeFileName (const std::string agentName, std::string& fileName)
{
	string defaultPath = startup::StartUp::installDir() + "/etc/";
	string localPath = startup::StartUp::installDir() + "/local/";
	if (agentName.compare (MASTER_AGENT) == 0) {
		fileName = defaultPath;
		fileName += "snmpd.conf";
	}
	else if (agentName.compare (TRAPD) == 0) {
		fileName = defaultPath;
		fileName += "snmptrapd.conf";
	}
	else {
		fileName = localPath;
		fileName += "snmpdx.conf";
	}
}

/*****************************************************************************************
* @brief	makeProcessName API
*
* purpose:	returns process name for AGENT
*
*****************************************************************************************/
void SNMPManager::makeProcessName (const std::string agentName, std::string& processName)
{
	if (agentName.compare (MASTER_AGENT) == 0) {
		processName = "SNMPParentAgent";
	}
	else if (agentName.compare (TRAPD) == 0) {
		processName = "SNMPTrapDaemon";
	}
	else {
		processName = "SNMPSubagent";
	}
}

/*****************************************************************************************
* @brief	updateSNMPD API
*
* purpose:	updates Parent OAm IP address in snmpd.conf file
*
*****************************************************************************************/
void SNMPManager::updateSNMPD(std::string oldIPAddr, std::string parentOAMModuleIPAddr)
{
	string fileName = startup::StartUp::installDir() + "/etc/snmpd.conf";

	ifstream oldFile (fileName.c_str());
	if (!oldFile) return;
	
	vector <string> lines;
	char line[200];
	string buf;
	string newLine;
	string newLine1;
	while (oldFile.getline(line, 200))
	{
		buf = line;
		string::size_type pos = buf.find(oldIPAddr,0);
		if (pos != string::npos)
		{
	        newLine = buf.substr(0, pos);
    	    newLine.append(parentOAMModuleIPAddr);

			newLine1 = buf.substr(pos + oldIPAddr.size(), 200);
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
	return;
}

} //namespace snmpmanager
// vim:ts=4 sw=4:

