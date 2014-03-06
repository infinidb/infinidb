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

/*
 * $Id: logger.cpp 4037 2013-08-07 03:37:31Z bwilkinson $
 */

#include <string>
using namespace std;
#include <boost/thread.hpp>
using namespace boost;

#include "logger.h"

namespace logging
{

Logger::Logger(unsigned subsys) :
	fMl1(LoggingID(subsys))
{
}

const string Logger::logMessage(LOG_TYPE logLevel, Message::MessageID mid, const Message::Args& args,
	const LoggingID& logInfo)
{
	MsgMap::mapped_type msg;
	MsgMap::const_iterator msgIter = fMsgMap.find(mid);

	//Default message if specified # not found
	if (msgIter == fMsgMap.end())
		msg = Message(M0000);
	else
		msg = msgIter->second;

	msg.reset();
	msg.format(args);

	return logMessage(logLevel, msg, logInfo);
	/*
	mutex::scoped_lock lk(fLogLock);
	fMl1.logData(logInfo);

	switch (logLevel)
	{
        case LOG_TYPE_DEBUG:
	default:
		fMl1.logDebugMessage(msg);
		break;
        case LOG_TYPE_INFO:
		fMl1.logInfoMessage(msg);
		break;
        case LOG_TYPE_WARNING:
		fMl1.logWarningMessage(msg);
		break;
        case LOG_TYPE_ERROR:
		fMl1.logErrorMessage(msg);
		break;
        case LOG_TYPE_CRITICAL:
		fMl1.logCriticalMessage(msg);
		break;
	}

	return  msg.msg();*/
}

const std::string Logger::logMessage(LOG_TYPE logLevel, const Message& msg, const LoggingID& logInfo)
{
	mutex::scoped_lock lk(fLogLock);
	fMl1.logData(logInfo);

	switch (logLevel)
	{
		case LOG_TYPE_DEBUG:
		default:
			fMl1.logDebugMessage(msg);
			break;
		case LOG_TYPE_INFO:
			fMl1.logInfoMessage(msg);
			break;
		case LOG_TYPE_WARNING:
			fMl1.logWarningMessage(msg);
			break;
		case LOG_TYPE_ERROR:
			fMl1.logErrorMessage(msg);
			break;
		case LOG_TYPE_CRITICAL:
			fMl1.logCriticalMessage(msg);
			break;
	}

	return  msg.msg();
}

}

