/***************************************************************************
 * $Id: serverMonitor.cpp 34 2006-09-29 21:13:54Z dhill $
 *
 *   Author: David Hill
 ***************************************************************************/

#include "serverMonitor.h"
#include "installdir.h"

using namespace std;
using namespace oam;
using namespace snmpmanager;
using namespace logging;
using namespace servermonitor;

namespace servermonitor {


/******************************************************************************************
* @brief	ServerMonitor Constructor
*
* purpose:	ServerMonitor Constructor
*
******************************************************************************************/
ServerMonitor::ServerMonitor()
{
}

/******************************************************************************************
* @brief	ServerMonitor Destructor
*
* purpose:	ServerMonitor Destructor
*
******************************************************************************************/
ServerMonitor::~ServerMonitor()
{
}

/******************************************************************************************
* @brief	sendAlarm
*
* purpose:	send a trap and log the process information
*
******************************************************************************************/
void ServerMonitor::sendAlarm(string alarmItem, ALARMS alarmID, int action, float sensorValue)
{
	ServerMonitor serverMonitor;
	Oam oam;

	//Log this event 
	LoggingID lid(SERVER_MONITOR_LOG_ID);
	MessageLog ml(lid);
	Message msg;
	Message::Args args;
	args.add(alarmItem);
	args.add(", sensor value out-of-range: ");
	args.add(sensorValue);

	// get current server name
	string moduleName;
	oamModuleInfo_t st;
	try {
		st = oam.getModuleInfo();
		moduleName = boost::get<0>(st);
	}
	catch (...) {
		moduleName = "Unknown Server";
	}

	// check if there is an active alarm above the reporting theshold 
	// that needs to be cleared
	serverMonitor.checkAlarm(alarmItem, alarmID);

	// check if Alarm is already active, don't resend
	if ( !( oam.checkActiveAlarm(alarmID, moduleName, alarmItem)) ) {

		SNMPManager alarmMgr;
		// send alarm
		alarmMgr.sendAlarmReport(alarmItem.c_str(), alarmID, action);

		args.add(", Alarm set: ");
		args.add(alarmID);
	}

	// output log
	msg.format(args);
	ml.logWarningMessage(msg);

	return;
}

/******************************************************************************************
* @brief	checkAlarm
*
* purpose:	check to see if an alarm(s) is set on device and clear if so
*
******************************************************************************************/
void ServerMonitor::checkAlarm(string alarmItem, ALARMS alarmID)
{
	Oam oam;

	// get current server name
	string moduleName;
	oamModuleInfo_t st;
	try {
		st = oam.getModuleInfo();
		moduleName = boost::get<0>(st);
	}
	catch (...) {
		moduleName = "Unknown Server";
	}

	switch (alarmID) {
		case NO_ALARM: 	// clear all alarms set if any found
			if ( oam.checkActiveAlarm(HARDWARE_HIGH, moduleName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, HARDWARE_HIGH);
			if ( oam.checkActiveAlarm(HARDWARE_MED, moduleName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, HARDWARE_MED);
			if ( oam.checkActiveAlarm(HARDWARE_LOW, moduleName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, HARDWARE_LOW);
			break;
		case HARDWARE_LOW: 	// clear high and medium alarms set if any found
			if ( oam.checkActiveAlarm(HARDWARE_HIGH, moduleName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, HARDWARE_HIGH);
			if ( oam.checkActiveAlarm(HARDWARE_MED, moduleName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, HARDWARE_MED);
			break;
		case HARDWARE_MED: 	// clear high alarms set if any found
			if ( oam.checkActiveAlarm(HARDWARE_HIGH, moduleName, alarmItem) )
				//  alarm set, clear it
				clearAlarm(alarmItem, HARDWARE_HIGH);
			break;
		default:			// none to clear
			break;
		} // end of switch
	return;
}

/******************************************************************************************
* @brief	clearAlarm
*
* purpose:	clear Alarm that was previously set
*
******************************************************************************************/
void ServerMonitor::clearAlarm(string alarmItem, ALARMS alarmID)
{
	SNMPManager alarmMgr;
	alarmMgr.sendAlarmReport(alarmItem.c_str(), alarmID, CLEAR);

	//Log this event 
	LoggingID lid(SERVER_MONITOR_LOG_ID);
	MessageLog ml(lid);
	Message msg;
	Message::Args args;
	args.add(alarmItem);
	args.add(" alarm #");
	args.add(alarmID);
	args.add("cleared");
	msg.format(args);
	ml.logWarningMessage(msg);
}

/******************************************************************************************
* @brief	sendMsgShutdownServer
*
* purpose:	send a Message to Shutdown server
*
******************************************************************************************/
/*void ServerMonitor::sendMsgShutdownServer()
{
	Oam oam;

	//Log this event 
	LoggingID lid(SERVER_MONITOR_LOG_ID);
	MessageLog ml(lid);
	Message msg;
	Message::Args args;
	args.add("serverMonitor: Fatal Hardware Alarm detected, Server being shutdown");
	msg.format(args);
	ml.logCriticalMessage(msg);

	string moduleName;
	oamModuleInfo_t st;
	try {
		st = oam.getModuleInfo();
		moduleName = boost::get<0>(st);
	}
	catch (...) {
		// o well, let's take out own action
			system("init 0");
	}

	try
	{
		oam.shutdownModule(moduleName, FORCEFUL, ACK_NO);
	}
	catch (exception& e)
	{
		// o well, let's take out own action
			system("init 0");
	}
}
*/
/******************************************************************************************
* @brief	StripWhitespace
*
* purpose:	strip off whitespaces from a string
*
******************************************************************************************/
string ServerMonitor::StripWhitespace(string value)
{
	for(;;)
	{
		string::size_type pos = value.find (' ',0);
		if (pos == string::npos)
			// no more found
			break;
		// strip leading
		if (pos == 0) {
			value = value.substr (pos+1,10000);
		}
		else 
		{ // strip trailing
			value = value.substr (0, pos);
		}
	}
	return value;
}


/******************************************************************************************
* @brief	sendResourceAlarm
*
* purpose:	send a trap and log the process information
*
******************************************************************************************/
bool ServerMonitor::sendResourceAlarm(string alarmItem, ALARMS alarmID, int action, int usage)
{
	ServerMonitor serverMonitor;
	Oam oam;

	//Log this event 
	LoggingID lid(SERVER_MONITOR_LOG_ID);
	MessageLog ml(lid);
	Message msg;
	Message::Args args;
	args.add(alarmItem);
	args.add(" usage at percentage of ");
	args.add(usage);

	// get current module name
	string moduleName;
	oamModuleInfo_t st;
	try {
		st = oam.getModuleInfo();
		moduleName = boost::get<0>(st);
	}
	catch (...) {
		moduleName = "Unknown Server";
	}

	// check if there is an active alarm above the reporting theshold 
	// that needs to be cleared

	if (alarmItem == "CPU")
		serverMonitor.checkCPUAlarm(alarmItem, alarmID);
	else if (alarmItem == "Local Disk" || alarmItem == "External")
			serverMonitor.checkDiskAlarm(alarmItem, alarmID);
	else if (alarmItem == "Local Memory")
			serverMonitor.checkMemoryAlarm(alarmItem, alarmID);
	else if (alarmItem == "Local Swap")
			serverMonitor.checkSwapAlarm(alarmItem, alarmID);

	// don't issue an alarm on thge dbroots is already issued by this or another server
	if ( alarmItem.find(startup::StartUp::installDir() + "/data") == 0 ) {
		// check if Alarm is already active from any module, don't resend
		if ( !( oam.checkActiveAlarm(alarmID, "*", alarmItem)) ) {
	
			SNMPManager alarmMgr;
			// send alarm
			alarmMgr.sendAlarmReport(alarmItem.c_str(), alarmID, action);
	
			args.add(", Alarm set: ");
			args.add(alarmID);
			msg.format(args);
			ml.logInfoMessage(msg);
			return true;
		}
		else
			return false;
	}
	else
	{
		// check if Alarm is already active from this module, don't resend
		if ( !( oam.checkActiveAlarm(alarmID, moduleName, alarmItem)) ) {
	
			SNMPManager alarmMgr;
			// send alarm
			alarmMgr.sendAlarmReport(alarmItem.c_str(), alarmID, action);
	
			args.add(", Alarm set: ");
			args.add(alarmID);
			msg.format(args);
			ml.logInfoMessage(msg);
			return true;
		}
		else
			return false;
	}

	return true;
}


} // end of namespace
