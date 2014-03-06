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

#include <string>
using namespace std;

#include "messageobj.h"
#include "messageids.h"
#include "loggingid.h"
#include "sqllogger.h"

namespace logging
{

const unsigned LogDefaultMsg = M0000;
const unsigned LogStartSql = M0041;
const unsigned LogEndSql = M0042;

//We can't use a member Logger here.  When used with DML and DDL, the syslog gets closed by calls to DMLLog and DDLLog.
SQLLogger::SQLLogger(const std::string sql, unsigned subsys, unsigned session, unsigned txn, unsigned thread)
	  : fLogId(subsys, session, txn, thread), fLog(!sql.empty())
{
	makeMsgMap();
	if (fLog) logMessage(LOG_TYPE_DEBUG, sql, LogStartSql);
}

SQLLogger::SQLLogger(const std::string sql, const LoggingID& logId)
	: fLogId(logId.fSubsysID, logId.fSessionID, logId.fTxnID, logId.fThdID), fLog(!sql.empty())
{
	makeMsgMap();
	if (fLog) logMessage(LOG_TYPE_DEBUG, sql, LogStartSql);
}

void SQLLogger::makeMsgMap()
{
	fMsgMap[LogDefaultMsg] = Message(LogDefaultMsg);
	fMsgMap[LogStartSql] = Message(LogStartSql);
	fMsgMap[LogEndSql] = Message(LogEndSql);
}

SQLLogger::~SQLLogger()
{	
	if (fLog) logMessage(LOG_TYPE_DEBUG, "", LogEndSql);
}

std::string SQLLogger::logMessage(logging::LOG_TYPE logLevel, const std::string& msg, logging::Message::MessageID mid )
{
	logging::Message::Args args;
	args.add(msg);
	Logger logger(fLogId.fSubsysID);
	logger.msgMap(fMsgMap);
	return logger.logMessage(logLevel, mid, args, fLogId);
}

}
