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
