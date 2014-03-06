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
 * $Id: hardwareMonitor.h 34 2006-09-29 21:13:54Z dhill $
 *
 *   Author: David Hill
 ***************************************************************************/
/**
 * @file
 */
#ifndef HARDWARE_MONITOR_H
#define HARDWARE_MONITOR_H

#include <iostream>
#include <fstream>
#include <iomanip>
#include <cstdlib>
#include <cerrno>
#include <exception>
#include <stdexcept>

#include "liboamcpp.h"
#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
#include "snmpmanager.h"


int IPMI_SUPPORT = 0;	// 0 for supported

/**
 * @brief send alarm
 */
void sendAlarm(std::string alarmItem, oam::ALARMS alarmID, int action, float sensorValue);

/**
 * @brief check alarm
 */
void checkAlarm(std::string alarmItem, oam::ALARMS alarmID = oam::NO_ALARM);

/**
 * @brief clear alarm
 */
void clearAlarm(std::string alarmItem, oam::ALARMS alarmID);

/**
 * @brief send msg to shutdown server
 */
void sendMsgShutdownServer();

/**
 * @brief strip off whitespaces from a string
 */
std::string StripWhitespace(std::string value);

#endif
