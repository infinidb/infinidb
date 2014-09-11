/******************************************************************************************
* $Id: trapHandler.cpp 1970 2011-08-19 17:37:17Z dhill $
*
* Author: Zhixuan Zhu
******************************************************************************************/
#include <cstdio>
#include <iostream>
#include <string>
#include <sys/file.h>
#include <cerrno>
#include <exception>
#include <stdexcept>
#include "snmpmanager.h"
#include "alarm.h"
#include "liboamcpp.h"
#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"

using namespace std;
using namespace snmpmanager;
using namespace oam;
using namespace logging;

/**
 * constants define
 */ 
const int DEBUG = 0;
const string errMsg = "Not valid alarm data";
const char* DELIM = "|";
const char* AGENT_TRAP = "agentTrap";
const unsigned int CTN_INTERVAL = 30*60;

/**
 * @brief get next token of alarm data from input stream
 */  
char* getNextToken ();

/**
 * @brief set alarm data from alarm configuration file
 */ 
void configAlarm (Alarm &);

/**
 * @brief process alarm
 */
void processAlarm (Alarm &);

/**
 * @brief log alarm to historical alarm file
 */
void logAlarm (const Alarm &, string fileName);

/**
 * @brief rewrite active alarm file after removing alarm
 */
void rewriteActiveLog (AlarmList &);

/*****************************************************************************************
* @brief	main function
*
* purpose:	Parse incoming ALARM statement into calAlarm class
*
*****************************************************************************************/
int main (int argc, char *argv[]) {

	if (argc != 2)
  		exit (0);
  
  	Alarm calAlarm;

  	char buf[100];
  	char* alarmData;
  	char* token;
  	bool successFlag = false;

  	if (::DEBUG) {
		LoggingID lid(11);
		MessageLog ml(lid);
		Message msg;
		Message::Args args;
		args.add("trapHandler Launched");
		msg.format(args);
		ml.logDebugMessage(msg);
	}

	// read alarm data
	while (cin.getline(buf,100))
  	{
		// Alarm data
		if (::DEBUG) {
			LoggingID lid(11);
        		MessageLog ml(lid);
        		Message msg;
        		Message::Args args;
        		args.add("Alarm Data:");
			args.add(buf);
        		msg.format(args);
        		ml.logDebugMessage(msg);
		}
  		// search for CALALARM
  		if ((alarmData = strstr(buf, "CALALARM")) == NULL)
  			continue;
  		
  		successFlag = true;
  		token = strtok (alarmData, DELIM);
  		
  		// alarmData format: CALALARM|alarmID|componentID|1(set)/0(clear)|server|process
  		// alarmID
		try {
  			token = getNextToken();
		} catch (runtime_error e)
		{
			if (::DEBUG) {                                
				LoggingID lid(11);                                
				MessageLog ml(lid);                                
				Message msg;                                
				Message::Args args;                                
				args.add("getNextToken error:");                                
				args.add(e.what());                                
				msg.format(args);                                
				ml.logDebugMessage(msg);                                
			}
			exit(1);
		}

  		calAlarm.setAlarmID (atoi(token));
  		
  		// componentID
		try {
  			token = getNextToken();
		} catch (runtime_error e)
		{
			if (::DEBUG) {                                
				LoggingID lid(11);                                
				MessageLog ml(lid);                                
				Message msg;                                
				Message::Args args;                                
				args.add("getNextToken error:");                                
				args.add(e.what());                                
				msg.format(args);                                
				ml.logDebugMessage(msg);                                
			}
			exit(1);
		}
  		calAlarm.setComponentID (token);
  		
  		// state
		try {
  			token = getNextToken();
		} catch (runtime_error e)
		{
			if (::DEBUG) {                                
				LoggingID lid(11);                                
				MessageLog ml(lid);                                
				Message msg;                                
				Message::Args args;                                
				args.add("getNextToken error:");                                
				args.add(e.what());                                
				msg.format(args);                                
				ml.logDebugMessage(msg);                                
			}
			exit(1);
		}
  		calAlarm.setState (atoi(token));
  		
		// sname
		try {
  			token = getNextToken();
		} catch (runtime_error e)
		{
			if (::DEBUG) {                                
				LoggingID lid(11);                                
				MessageLog ml(lid);                                
				Message msg;                                
				Message::Args args;                                
				args.add("getNextToken error:");                                
				args.add(e.what());                                
				msg.format(args);                                
				ml.logDebugMessage(msg);                                
			}
			exit(1);
		}
		calAlarm.setSname (token);

		// pname
		try {
  			token = getNextToken();
		} catch (runtime_error e)
		{
			if (::DEBUG) {                                
				LoggingID lid(11);                                
				MessageLog ml(lid);                                
				Message msg;                                
				Message::Args args;                                
				args.add("getNextToken error:");                                
				args.add(e.what());                                
				msg.format(args);                                
				ml.logDebugMessage(msg);                                
			}
			exit(1);
		}
		calAlarm.setPname (token);

  		// distinguish agent trap and process trap.
  		// agent trap set pid and tid 0.
  		if (strcmp (argv[1], AGENT_TRAP) == 0)
  		{
  			calAlarm.setPid (0);
  			calAlarm.setTid (0);
  		}
  		// process trap continues to get pid from alarm data
  		else
  		{
  			// pid
			try {
				token = getNextToken();
			} catch (runtime_error e)
			{
				if (::DEBUG) {                                
					LoggingID lid(11);                                
					MessageLog ml(lid);                                
					Message msg;                                
					Message::Args args;                                
					args.add("getNextToken error:");                                
					args.add(e.what());                                
					msg.format(args);                                
					ml.logDebugMessage(msg);                                
				}
				exit(1);
			}
			calAlarm.setPid (atoi(token));

  			// tid
			try {
				token = getNextToken();
			} catch (runtime_error e)
			{
				if (::DEBUG) {                                
					LoggingID lid(11);                                
					MessageLog ml(lid);                                
					Message msg;                                
					Message::Args args;                                
					args.add("getNextToken error:");                                
					args.add(e.what());                                
					msg.format(args);                                
					ml.logDebugMessage(msg);                                
				}
				exit(1);
			}
  			calAlarm.setTid (atoi(token)); 
  		}

  		if (::DEBUG){
        		LoggingID lid(11);
        		MessageLog ml(lid);
        		Message msg;
        		Message::Args args;
        		args.add("Alarm Info:");
			args.add(calAlarm.getAlarmID());
			args.add(calAlarm.getComponentID());
			args.add(calAlarm.getState());
        		msg.format(args);
        		ml.logDebugMessage(msg);

  			cout << calAlarm.getAlarmID() << ":" 
  				 << calAlarm.getComponentID() << ":"
  				 << calAlarm.getState() << endl;
		}
  		// break while loop. ignore the other info carried by
  		// the trap. May need to retrieve more info in the future.
  		break;
	}
  
  // not valid alarm data if no "CALALARM" found
  if (!successFlag){
	LoggingID lid(11);                
	MessageLog ml(lid);                
	Message msg;                
	Message::Args args;                
	args.add("Error: not valid alarm data if no 'CALALARM' found");                
	msg.format(args);                
	ml.logDebugMessage(msg); 
	exit(1);
  }

  // Get alarm configuration
	try {
  		configAlarm (calAlarm);
	} catch (runtime_error e)
	{
		if (::DEBUG) {                                
			LoggingID lid(11);                                
			MessageLog ml(lid);                                
			Message msg;                                
			Message::Args args;                                
			args.add("configAlarm error:");                                
			args.add(e.what());                                
			msg.format(args);                                
			ml.logDebugMessage(msg);                                
		}
		exit(1);
	}

  return 0;
}

/*****************************************************************************************
* @brief	getNextToken
*
* purpose:	Get next token in incoming Alarm Statement
*
*****************************************************************************************/
char* getNextToken()
{
	char* token = strtok (NULL, DELIM);
  	if (token == NULL)
  	{
  		// TODO: call system log api to log the error
		throw runtime_error (errMsg);
	}
	return token;
}

/*****************************************************************************************
* @brief	configAlarm
*
* purpose:	Get Config Data for Incoming alarm
*
*****************************************************************************************/
void configAlarm (Alarm &calAlarm)
{
	int alarmID = calAlarm.getAlarmID();
	Oam oam;
	AlarmConfig alarmConfig;

        if (::DEBUG) {                        
		LoggingID lid(11);                        
		MessageLog ml(lid);                        
		Message msg;                        
		Message::Args args;                        
		args.add("configAlarm Called");                        
		msg.format(args);                        
		ml.logDebugMessage(msg);                
	}

	try  	
	{ 
  		oam.getAlarmConfig (alarmID, alarmConfig);
  	  	
  		calAlarm.setDesc (alarmConfig.BriefDesc);
  		calAlarm.setSeverity (alarmConfig.Severity);
  		calAlarm.setCtnThreshold (alarmConfig.Threshold);
  		calAlarm.setOccurrence (alarmConfig.Occurrences);
  		calAlarm.setLastIssueTime (alarmConfig.LastIssueTime); 
  
	  	// check lastIssueTime to see if it's time to clear the counter
  		time_t now;
  		time (&now);
  		if ((now - calAlarm.getLastIssueTime()) >= CTN_INTERVAL)
  		{
  			// reset counter and set lastIssueTime
  			oam.setAlarmConfig (alarmID, "LastIssueTime", now);
  			oam.setAlarmConfig (alarmID, "Occurrences", 1);
  		}
  
  		else
  		{
  			// increment counter and check the ctnThreshold
  			calAlarm.setOccurrence (alarmConfig.Occurrences+1);
  			oam.setAlarmConfig (alarmID, "Occurrences", calAlarm.getOccurrence());
  		
  			// if counter over threshold and set alarm, stop processing.
  			if (calAlarm.getCtnThreshold() > 0 
  				&& calAlarm.getOccurrence() >= calAlarm.getCtnThreshold()
  				&& calAlarm.getState() == SET)
  			{
        			if (::DEBUG) {                
					LoggingID lid(11);                
					MessageLog ml(lid);                
					Message msg;                
					Message::Args args;                
					args.add("counter over threshold and set alarm, stop processing.");                
					args.add("threshold:");
					args.add(calAlarm.getCtnThreshold());
					args.add("occurances:");
					args.add(calAlarm.getOccurrence());
					msg.format(args);                
					ml.logDebugMessage(msg);        
				}
  				return;
  			}
  		}
  	} catch (runtime_error e)
  	{
  		if (::DEBUG) {                                
			LoggingID lid(11);                                
			MessageLog ml(lid);                                
			Message msg;                                
			Message::Args args;                                
			args.add("runtime error:");                                
			args.add(e.what());                                
			msg.format(args);                                
			ml.logDebugMessage(msg);                                
		}
		throw e;
  	}
  	  		
	// process alarm
	processAlarm (calAlarm);
}

/*****************************************************************************************
* @brief	processAlarm
*
* purpose:	Process Alarm by updating Active Alarm  and Historical Alarm files
*
*****************************************************************************************/
void processAlarm (Alarm &calAlarm)
{
	bool logFlag = (calAlarm.getState() == CLEAR ? false: true);
	
	// get active alarms
	AlarmList alarmList;
	SNMPManager sm;
	sm.getActiveAlarm (alarmList);
	
        if (::DEBUG) {                                        
		LoggingID lid(11);                                        
		MessageLog ml(lid);                                        
		Message msg;                                        
		Message::Args args;                                        
		args.add("processAlarm Called");                                 
		msg.format(args);                                        
		ml.logDebugMessage(msg);                        
	}
	
	AlarmList::iterator i;
   	for (i = alarmList.begin(); i != alarmList.end(); ++i)
   	{
		// check if matching ID
  		if (calAlarm.getAlarmID() != (i->second).getAlarmID() ) {
			continue;
		}
   		// check if the same fault component on same server
   		if (calAlarm.getComponentID().compare((i->second).getComponentID()) == 0 &&
			calAlarm.getSname().compare((i->second).getSname()) == 0)
        {
   			// for set alarm, don't log to active
           	if (calAlarm.getState() == SET )
           	{
           		logFlag = false;
           		break;
           	}

           	// for clear alarm, remove the set by rewritting the file
           	else if (calAlarm.getState() == CLEAR )
           	{
           		logFlag = false;
           		//cout << "size before: " << alarmList.size();
           		alarmList.erase (i);
           		//cout << " size after: " << alarmList.size() << endl;
				try {
           			rewriteActiveLog (alarmList);
				} catch (runtime_error e)
				{
					if (::DEBUG) {                                
						LoggingID lid(11);                                
						MessageLog ml(lid);                                
						Message msg;                                
						Message::Args args;                                
						args.add("rewriteActiveLog error:");                                
						args.add(e.what());                                
						msg.format(args);                                
						ml.logDebugMessage(msg);                                
					}
					exit(1);
				}
           		break;
           	}
        }
   	} // end of for loop

   	if (logFlag) {
		try {
   			logAlarm (calAlarm, ACTIVE_ALARM_FILE);
		} catch (runtime_error e)
		{
			if (::DEBUG) {                                
				LoggingID lid(11);                                
				MessageLog ml(lid);                                
				Message msg;                                
				Message::Args args;                                
				args.add("logAlarm error:");                                
				args.add(e.what());                                
				msg.format(args);                                
				ml.logDebugMessage(msg);                                
			}
			exit(1);
		}
	}
	
	// log historical alarm
	try {
		logAlarm (calAlarm, ALARM_FILE);
	} catch (runtime_error e)
	{
		if (::DEBUG) {                                
			LoggingID lid(11);                                
			MessageLog ml(lid);                                
			Message msg;                                
			Message::Args args;                                
			args.add("logAlarm error:");                                
			args.add(e.what());                                
			msg.format(args);                                
			ml.logDebugMessage(msg);                                
		}
		exit(1);
	}

}

/*****************************************************************************************
* @brief	logAlarm
*
* purpose:	Log Alarm in Active Alarm or Historical Alarm file
*
*****************************************************************************************/
void logAlarm (const Alarm &calAlarm, string fileName)
{
        if (::DEBUG) {                                        
		LoggingID lid(11);                                        
		MessageLog ml(lid);                                        
		Message msg;                                        
		Message::Args args;                                        
		args.add("logAlarm Called");                                 
		msg.format(args);                                        
		ml.logDebugMessage(msg);                        
	}

	int fd = open(fileName.c_str(), O_RDWR|O_CREAT, 0666);
	ofstream AlarmFile (fileName.c_str(), ios::app);

	// Aquire an exclusive lock
   	if (flock(fd,LOCK_EX) == -1) {
    	throw runtime_error ("Lock file error: " + fileName);
   	}

	AlarmFile << calAlarm;
	AlarmFile.close();
	
	// Release lock
	if (flock(fd,LOCK_UN)==-1)
	{
    	throw runtime_error ("Release lock file error: " + fileName);		
	}

	close(fd);
}

/*****************************************************************************************
* @brief	rewriteActiveLog
*
* purpose:	Update Active Alarm file, called to remove Cleared alarm
*
*****************************************************************************************/
void rewriteActiveLog (AlarmList &alarmList)
{
	if (::DEBUG) {                                        
		LoggingID lid(11);                                        
		MessageLog ml(lid);                                        
		Message msg;                                        
		Message::Args args;                                        
		args.add("rewriteAlarmLog Called");                                 
		msg.format(args);                                        
		ml.logDebugMessage(msg);                        
	}

	// delete the old file
	unlink (ACTIVE_ALARM_FILE.c_str());
	
	// create new file
	int fd = open(ACTIVE_ALARM_FILE.c_str(), O_RDWR|O_CREAT, 0666);
	
	// Aquire an exclusive lock
   	if (flock(fd,LOCK_EX) == -1) {
    	throw runtime_error ("Lock active alarm log file error");
   	}

   	ofstream activeAlarmFile (ACTIVE_ALARM_FILE.c_str());
   	
   	AlarmList::iterator i;
   	for (i = alarmList.begin(); i != alarmList.end(); ++i)
   	{
   		activeAlarmFile << i->second;
   	}

   	activeAlarmFile.close();

   	// Release lock
	if (flock(fd,LOCK_UN)==-1)
	{
    	throw runtime_error ("Release lock active alarm log file error");		
	}
	close(fd);
}
