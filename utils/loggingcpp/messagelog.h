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
* $Id: messagelog.h 3495 2013-01-21 14:09:51Z rdempsey $
*
******************************************************************************************/
/**
 * @file
 */
#ifndef LOGGING_MESSAGELOG_H
#define LOGGING_MESSAGELOG_H

#include <syslog.h>

#include "loggingid.h"

class MessageLoggingTest;

namespace logging {

	/** @brief Log Types
  	*/
    enum LOG_TYPE
    {
        LOG_TYPE_DEBUG,		// 0 = debug
        LOG_TYPE_INFO,		// 1 = info
        LOG_TYPE_WARNING,	// 2 = warning
        LOG_TYPE_ERROR,		// 3 = error
        LOG_TYPE_CRITICAL	// 4 = critical
	};


class Message;

/** @brief a message log class
 *
 */
class MessageLog
{
public:
	/** @brief ctor
	*   @param initData the LogginID object.
	*   @param facility openLog will be called using this as the third parm.
	*/
	MessageLog(const LoggingID& initData, int facility=LOG_LOCAL1);

	/** @brief dtor
	 *
	 */
	~MessageLog();

	/** @brief log a debug message
	*
	* @param msg the message to log
	*/
	void logDebugMessage(const Message& msg);

	/** @brief log an info message
	*
	* @param msg the message to log
	*/
	void logInfoMessage(const Message& msg);

	/** @brief log a warning message
	*
	* @param msg the message to log
	*/
	void logWarningMessage(const Message& msg);

	/** @brief log an error message
	*
	* @param msg the message to log
	*/
	void logErrorMessage(const Message& msg);

	/** @brief log a serious message
	*
	* @param msg the message to log
	* @note this is the same as calling logErrorMessage()
	* @bug 24 add logErrorMessage() and make logSeriousMessage() call it
	*/
	void logSeriousMessage(const Message& msg) { logErrorMessage(msg); }

	/** @brief log a critial message
	*
	* @param msg the message to log
	*/
	void logCriticalMessage(const Message& msg);

	/** @brief LoggingID mutator
	*
	* @param logData the new LoggingID. If the subsystem id is changed, the syslog connection will be closed
	*  and reopened.
	*/
	void logData(const LoggingID& logData);

	/** @brief LoggingID accessor
	*
	*/
	const LoggingID& logData() const { return fLogData; }

	friend class ::MessageLoggingTest;

protected:

	const std::string format(const Message& msg, const char prefix='U');

private:
	//defaults okay
	//MessageLog(const MessageLog& rhs);
	//MessageLog& operator=(const MessageLog& rhs);

	LoggingID fLogData; /// the logging context data
	int fFacility; /// the syslog facility number
};

/** @brief logs the DML statement using a Syslog critical message to LOG_LOCAL2.
*/
void logDML(unsigned sessionId, unsigned txnId, const std::string& statement, const std::string& owner);

/** @brief logs the DDL statement using a Syslog critical message to LOG_LOCAL2.
*/
void logDDL(unsigned sessionId, unsigned txnId, const std::string& statement, const std::string& owner);

/** @brief logs the commit or rollback statement using a Syslog critical message to LOG_LOCAL2.
*/
void logCommand(unsigned sessionId, unsigned txnId, const std::string& statement);

/** @brief logs the event using a Syslog critical message to LOG_LOCAL2.  Used for messages 19..24.
*/
void logEventToDataLog(unsigned messageId, const std::string& messageText);

}

#endif
