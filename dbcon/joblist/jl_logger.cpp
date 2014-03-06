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
 * $Id: jl_logger.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 */

#include <string>
using namespace std;

#include <boost/thread/mutex.hpp>

#include "messageobj.h"
#include "messageids.h"
#include "loggingid.h"
using namespace logging;

#include "jl_logger.h"

namespace
{
boost::mutex logMutex;
};


namespace joblist
{

Logger::Logger() : fLogId(5),
	fImpl(new logging::Logger(5))
{
	MsgMap msgMap;

	msgMap[LogDefaultMsg] = Message(LogDefaultMsg);
	msgMap[LogSQLTrace] = Message(LogSQLTrace);
	msgMap[LogNoPrimProcs] = Message(LogNoPrimProcs);
	msgMap[LogMakeJobList] = Message(LogMakeJobList);
	msgMap[LogRDRequest] = Message(LogRDRequest);
	msgMap[LogRDRequestWait] = Message(LogRDRequestWait);
	msgMap[LogRDReturn] = Message(LogRDReturn);
	msgMap[LogRMResourceChange] = Message(LogRMResourceChange);
	msgMap[LogRMResourceChangeError] = Message(LogRMResourceChangeError);

	fImpl->msgMap(msgMap);
}

void catchHandler(const string& ex, int c, SErrorInfo& ei, unsigned sid, logging::LOG_TYPE level)
{
	boost::mutex::scoped_lock lk(logMutex);
	if (ei->errCode == 0)
	{
		ei->errMsg = ex;
		ei->errCode = c;
	}

	Logger log;
	log.setLoggingSession(sid);
	log.logMessage(level, ex);
}

const string Logger::logMessage(logging::LOG_TYPE logLevel, unsigned idbErrorCode)
{
	string errMsg = logging::IDBErrorInfo::instance()->errorMsg(idbErrorCode);
	logMessage(logLevel, errMsg);
	return errMsg;
}

}
