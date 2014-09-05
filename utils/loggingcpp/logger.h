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
 * $Id: logger.h 3495 2013-01-21 14:09:51Z rdempsey $
 */

/** @file */

#ifndef LOGGING_LOGGER_H_
#define LOGGING_LOGGER_H_

#include <string>
#include <map>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>

#include "messageobj.h"
#include "messagelog.h"
#include "messageids.h"

namespace logging
{

typedef std::map<Message::MessageID, Message> MsgMap;

/** @brief message log wrapper class */
class Logger
{
public:
	/** @brief ctor
	*
	* @param subsys the subsystem id
	*/
	explicit Logger(unsigned subsys);

	/** @brief log a message
	*
	* Log a message at a certain debug level
	*/
	const std::string logMessage(LOG_TYPE logLevel, Message::MessageID mid,
		const Message::Args& args, const LoggingID& logInfo);
	
	/** @brief log a formated message
	*
	* For the error framework to use
	*/
	const std::string logMessage(LOG_TYPE logLevel, const Message& message, const LoggingID& logInfo);

	/** @brief set the message map for this logger
	*
	* This method sets the message map for this logger. You need to call this (once and only once) before
	* using the class. The map needs to be constructed before and given to this class.
	*/
	void msgMap(const MsgMap& msgMap) { fMsgMap = msgMap; }

	/** @brief get the message map from this logger
	*/
	const MsgMap& msgMap() const { return fMsgMap; }

private:
	// not copyable (because of the mutex)
	Logger(const Logger& rhs);
	Logger& operator=(const Logger& rhs);

	MsgMap fMsgMap;
	MessageLog fMl1;
	boost::mutex fLogLock;
};

typedef boost::shared_ptr<Logger> SPL;

}

#endif

