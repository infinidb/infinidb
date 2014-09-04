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
* $Id: alarm.h 678 2012-04-04 18:20:46Z rdempsey $
*
* Author: Zhixuan Zhu
******************************************************************************************/
/**
 * @file
 */
#ifndef CAL_ALARM_H
#define CAL_ALARM_H

#include <string>
#include <stdint.h>
#include "snmpglobal.h"

namespace snmpmanager {

/** @brief Alarm class interface
 *
 */
class Alarm
{
public:
	/*
	 * @brief overloaded stream operator
	 */
	friend std::ostream &operator<< ( std::ostream &, const Alarm& );
	friend std::istream &operator>> ( std::istream &, Alarm & );
	
	/*
	 * @brief default ctor
	 */
	Alarm();

	/*
	 * @brief dtor
	 */
	virtual ~Alarm();
	
	/*
	 * @brief access methods
	 */
	inline const uint16_t getAlarmID() const
	{
		return alarmID;
	}
	void setAlarmID (const uint16_t);
	
	inline const std::string getDesc() const
	{
		return desc;
	}
	void setDesc (const std::string&);
	
	inline const std::string getComponentID() const
	{
		return componentID;
	}
	void setComponentID (const std::string&);
	
	inline const uint16_t getSeverity() const
	{
		return severity;
	}
	void setSeverity (const uint16_t);
	
	inline const bool getState () const
	{
		return state;
	}
	void setState (const bool);
	
	inline const uint16_t getCtnThreshold() const
	{
		return ctnThreshold;
	}
	void setCtnThreshold (const uint16_t);
	
	inline const uint16_t getOccurrence() const
	{
		return occurrence;
	}
	void setOccurrence (const uint16_t);
	
	inline const time_t& getReceiveTime () const
	{
		return receiveTime;
	}
	void setReceiveTime (const time_t);
	
	inline const uint32_t getLastIssueTime() const
	{
		return lastIssueTime;
	}
	void setLastIssueTime (const uint32_t);
	
	inline const uint16_t getPid () const
	{
		return pid;
	}
	void setPid (const uint16_t);
	
	inline const uint16_t getTid () const
	{
		return tid;
	}
	void setTid (const uint16_t);
	
	inline const std::string getTimestamp () const
	{
		return timestamp;
	}
	void setTimestamp (const std::string&);
	
	inline const time_t getTimestampSeconds () const
	{
		return timestampseconds;
	}
	void setTimestampSeconds (const time_t&);
	
	inline const std::string getSname () const
	{
		return sname;
	}
	void setSname (const std::string&);

	inline const std::string getPname () const
	{
		return pname;
	}
	void setPname (const std::string&);
	
	
private:
	uint16_t alarmID;
	std::string desc;
	std::string componentID;
	uint16_t severity;
	bool state;			// true: set; false: clear
	uint16_t ctnThreshold;
	uint16_t occurrence;
	time_t receiveTime;
	uint32_t lastIssueTime;
	uint16_t pid;		// report process id
	uint16_t tid;		// report thread id
	std::string sname;		// report server name
	std::string pname;		// report process name
	std::string timestamp;	// receive time in date/time format
	time_t timestampseconds;	// receive time in seconds format
};

}

#endif
