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
 * $Id: jl_logger.h 9210 2013-01-21 14:10:42Z rdempsey $
 */

/** @file */

#ifndef JOBLIST_LOGGER_H_
#define JOBLIST_LOGGER_H_

#include <boost/shared_ptr.hpp>

#include "messageids.h"
#include "messageobj.h"
#include "loggingid.h"
#include "errorids.h"
#include "logger.h"
#include "errorcodes.h"
#include "errorinfo.h"
#include "exceptclasses.h"

namespace joblist
{
const unsigned LogDefaultMsg = logging::M0000;
const unsigned LogSQLTrace = logging::M0036;
const unsigned LogNoPrimProcs = logging::M0043;
const unsigned LogMakeJobList = logging::M0059;
const unsigned LogRDRequest = logging::M0062;
const unsigned LogRDRequestWait = logging::M0063;
const unsigned LogRDReturn = logging::M0064;
const unsigned LogRMResourceChange = logging::M0066;
const unsigned LogRMResourceChangeError = logging::M0067;


/** @brief message log wrapper class */
class Logger
{
public:
	Logger();

	const std::string logMessage(logging::LOG_TYPE logLevel, logging::Message::MessageID mid,
		const logging::Message::Args& args, const logging::LoggingID& logInfo)
	{
		return  fImpl->logMessage(logLevel, mid, args, logInfo);
	}

	const std::string logMessage(logging::LOG_TYPE logLevel, const std::string& msg, logging::Message::MessageID mid = LogDefaultMsg )
	{
		logging::Message::Args args;
		args.add(msg);
		return  fImpl->logMessage(logLevel, mid, args, fLogId);
	}
	
	const std::string logMessage(logging::LOG_TYPE logLevel, unsigned idbErrorCode);

	void setLoggingSession(unsigned sid) { fLogId.fSessionID = sid; }
	void setLoggingTxn(unsigned txn) { fLogId.fTxnID = txn; }
	void setLoggingThd(unsigned thr) { fLogId.fThdID = thr; }
private:
	// defaults okay
	//Logger(const Logger& rhs);
	//Logger& operator=(const Logger& rhs);
	logging::LoggingID fLogId;

	logging::SPL fImpl;
};

typedef boost::shared_ptr<Logger> SPJL;
void catchHandler(const std::string& s, int c, SErrorInfo& errorInfo, unsigned sid = 0,
	logging::LOG_TYPE = logging::LOG_TYPE_CRITICAL);

}

#endif

