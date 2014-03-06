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

/***********************************************************************
 *   $Id:  $
 *
 *
 ***********************************************************************/

/** @file */

#ifndef LOGGING_SQLLOGGER_H_
#define LOGGING_SQLLOGGER_H_

#include <string>

#include "messageids.h"
#include "messageobj.h"
#include "loggingid.h"
#include "logger.h"

namespace logging
{
extern const unsigned LogDefaultMsg;
extern const unsigned LogStartSql;
extern const unsigned LogEndSql;

/** @brief writes sql start with sql statement in constructor and sql end in destructor in debug.log
	if the sql text is not empty
*/
class SQLLogger
{
public:
	SQLLogger(const std::string sql, unsigned subsys, unsigned session, unsigned txn = 0, unsigned thread = 0);
	SQLLogger(const std::string sql, const LoggingID& logId);

	~SQLLogger();	

	std::string logMessage(logging::LOG_TYPE logLevel, const std::string& msg, logging::Message::MessageID mid = LogDefaultMsg );

private:
	void makeMsgMap();
	MsgMap fMsgMap;
	LoggingID fLogId;
	bool fLog;
};

}
#endif

