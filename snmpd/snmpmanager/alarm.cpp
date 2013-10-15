/* Copyright (C) 2013 Calpont Corp.

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
#include <cstring>

#include "alarm.h"
#include "liboamcpp.h"

using namespace std;
using namespace oam;

namespace snmpmanager {

Alarm::Alarm()
{
	// alarm receive time
	Oam oam;
	setTimestamp (oam.getCurrentTime());
	time_t cal;
	time (&cal);	
	setTimestampSeconds (cal);
}

Alarm::~Alarm()
{
}

void Alarm::setAlarmID (const uint16_t id)
{
	alarmID = id;
}
	
void Alarm::setDesc (const string& d)
{
	desc = d;
}
	
void Alarm::setComponentID (const string& id)
{
	componentID = id;
}
	
void Alarm::setSeverity (const uint16_t s)
{
	severity = s;
}
	
void Alarm::setState (const bool s)
{
	state = s;
}
	
void Alarm::setCtnThreshold (const uint16_t ctn)
{
	ctnThreshold = ctn;
}
	
void Alarm::setOccurrence (const uint16_t o)
{
	occurrence = o;
}
	
void Alarm::setLastIssueTime (const uint32_t time)
{
	lastIssueTime = time;
}

void Alarm::setPid (const uint16_t p)
{
	pid = p;
}

void Alarm::setTid (const uint16_t t)
{
	tid = t;
}

void Alarm::setTimestamp (const string& t)
{
	timestamp = t;
}

void Alarm::setTimestampSeconds (const time_t& t)
{
	timestampseconds = t;
}

void Alarm::setSname (const string& s)
{
	sname = s;
}

void Alarm::setPname (const string& p)
{
	pname = p;
}

istream &operator >>(istream &input, Alarm &alarm)
{
	char buf[100] = {0};
	alarm.setAlarmID (INVALID_ALARM_ID);
	
	while (!input.eof() && strcmp (buf, "") == 0)	
	{
		input.getline (buf, 100);
	}
	if (input.eof())
		return input;
	
	// Alarm ID	
	alarm.setAlarmID (atoi (buf));
	
	// Severity
	input.getline (buf, 100);
	if (strstr (buf, "CRITICAL") != 0)
		alarm.setSeverity (CRITICAL);
	else if (strstr (buf, "MAJOR") != 0)
		alarm.setSeverity (MAJOR);
	else if (strstr (buf, "MINOR") != 0)
		alarm.setSeverity (MINOR);
	else if (strstr (buf, "WARNING") != 0)
		alarm.setSeverity (WARNING);
	else if (strstr (buf, "INFORMATIONAL") != 0)
		alarm.setSeverity (INFORMATIONAL);		
	else 
		alarm.setSeverity (NO_SEVERITY);
		
	// state
	if (strstr (buf, "CLEARED") != 0)
		alarm.setState (0);
	else
		alarm.setState (1);
	
	// Desc
	input.getline (buf, 100);
	alarm.setDesc (buf);
	
	// Timestamp
	input.getline (buf, 100); 
	alarm.setTimestamp (buf);
	
	// Timestamp Seconds
	input.getline (buf, 100);
	Oam oam;
	alarm.setTimestampSeconds (atoi(buf));
	
	// Reporting server name
	input.getline (buf, 100);
	alarm.setSname (buf);
	
	// Reporting process name
	input.getline (buf, 100);
	alarm.setPname (buf);
	
	// fault device name
	input.getline (buf, 100);
	alarm.setComponentID (buf);
	
	input.ignore (100, '\n');
	return input;
}

ostream &operator<< (ostream &output, const Alarm &alarm)
{
	output << alarm.getAlarmID() << endl;
	if (alarm.getState() == 0)
		output << "CLEARED ";
	switch (alarm.getSeverity())
	{
	case CRITICAL:
		output << "CRITICAL ALARM" << endl;
		break;
	case MAJOR:
		output << "MAJOR ALARM" << endl;
		break;
	case MINOR:
		output << "MINOR ALARM" << endl;	
		break;
	case WARNING:
		output << "WARNING ALARM" << endl;	
		break;
	case INFORMATIONAL:
		output << "INFORMATIONAL ALARM" << endl;	
		break;
	case NO_SEVERITY:
		output << "NO_SEVERITY ALARM" << endl;	
		break;
	}
	
	output << alarm.getDesc() << endl;
	output << alarm.getTimestamp() << endl;
	output << alarm.getTimestampSeconds() << endl;
	output << alarm.getSname() << endl;
	output << alarm.getPname() << endl;
	output << alarm.getComponentID() << endl;
	output << endl;
	
	return output;
}


} //namespace snmpmanager
