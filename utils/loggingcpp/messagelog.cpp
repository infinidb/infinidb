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
* $Id: messagelog.cpp 3048 2012-04-04 15:33:45Z rdempsey $
*
******************************************************************************************/
#include <sstream>
#include <iomanip>
#include <ctime>
#include <sys/time.h>
#include <syslog.h>
#include <iostream>
#include <unistd.h>
#include <vector>
#include <string>
using namespace std;

#include <boost/assign.hpp>
namespace ba=boost::assign;

#include "messagelog.h"
#include "loggingid.h"
#include "messageobj.h"
#include "messageids.h"

namespace {
using namespace logging;

/*
 * This list matches SubsystemIDs.txt, but has names matching Process Config DB
 */
const vector<string> SubsystemID = ba::list_of
	("Calpont")		// id =  0 default
	("ddljoblist")		// id =  1
	("ddlpackage")		// id =  2
	("dmlpackage")		// id =  3
	("execplan")		// id =  4
	("joblist")		// id =  5
	("resultset")		// id =  6
	("calpontConsole")	// id =  7
	("oamcpp")		// id =  8
	("ServerMonitor")	// id =  9
	("traphandler")		// id = 10
	("snmpmanager")		// id = 11
	("configcpp")		// id = 12
	("loggingcpp")		// id = 13
	("messageqcpp")		// id = 14
	("DDLProc")		// id = 15
	("ExeMgr")		// id = 16
	("ProcessManager")	// id = 17
	("ProcessMonitor")	// id = 18
	("writeengine")		// id = 19
	("DMLProc")		// id = 20
	("dmlpackageproc")	// id = 21
	("threadpool")		// id = 22
	("ddlpackageproc")	// id = 23
	("dbcon")		// id = 24
	("DiskManager")		// id = 25
	("RouteMsg")		// id = 26
	("SQLBuffMgr")		// id = 27
	("PrimProc")		// id = 28
	("controllernode")	// id = 29
	("workernode")		// id = 30
	("messagequeue")	// id = 31
	("writeengineserver")//id = 32
	("writeenginesplit")// id = 33
	;

string timestr()
{
	struct tm tm;
	struct timeval tv;
	gettimeofday(&tv, 0);
#ifdef _MSC_VER
	errno_t p = 0;
	time_t t = (time_t)tv.tv_sec;
	p = localtime_s(&tm, &t);
	if (p != 0)
		memset(&tm, 0, sizeof(tm));
#else
	localtime_r(&tv.tv_sec, &tm);
#endif

	ostringstream oss;
	oss << setfill('0')
		<< setw(2) << tm.tm_sec
		<< '.'
		<< setw(6) << tv.tv_usec
		;
	return oss.str();
}

void closeLog()
{
}

void openLog(unsigned subsystemid, int localLogNum)
{
	if (subsystemid >= SubsystemID.size())
		subsystemid = 0;
}

const string escape_pct(const string& in)
{
	string out(in);
	string::size_type pos;
	pos = out.find('%', 0);
	while (pos != string::npos)
	{
		out.replace(pos, 1, "%%");
		pos = out.find('%', pos+2);
	}
	return out;
}

}

namespace logging {

MessageLog::MessageLog(const LoggingID& initData, int localLogNum) :
	fLogData(initData), fFacility(localLogNum)
{
	openLog(fLogData.fSubsysID, fFacility);
}

void MessageLog::logData(const LoggingID& logData)
{
	if (fLogData.fSubsysID != logData.fSubsysID)
	{
		closeLog();
		openLog(logData.fSubsysID, fFacility);
	}
	fLogData = logData;
}

MessageLog::~MessageLog()
{
	closeLog();
}

const string MessageLog::format(const Message& msg, const char prefix)
{
	ostringstream oss;
	oss << timestr() << " |"
		<< fLogData.fSessionID
		<< '|' << fLogData.fTxnID
		<< '|' << fLogData.fThdID
		<< "| "
		<< prefix << ' ' << setw(2) << setfill('0') << fLogData.fSubsysID
		<< ' ' << msg.msg()
		;
	return escape_pct(oss.str());
} 

void MessageLog::logDebugMessage(const Message& msg)
{
	::openlog(SubsystemID[fLogData.fSubsysID].c_str(), 0|LOG_PID, fFacility);
	::syslog(LOG_DEBUG, format(msg, 'D').c_str());
	::closelog();
}

void MessageLog::logInfoMessage(const Message& msg)
{
	::openlog(SubsystemID[fLogData.fSubsysID].c_str(), 0|LOG_PID, fFacility);
	::syslog(LOG_INFO, format(msg, 'I').c_str());
	::closelog();
}

void MessageLog::logWarningMessage(const Message& msg)
{
	::openlog(SubsystemID[fLogData.fSubsysID].c_str(), 0|LOG_PID, fFacility);
	::syslog(LOG_WARNING, format(msg, 'W').c_str());
	::closelog();
}

void MessageLog::logErrorMessage(const Message& msg)
{
	// @bug 24 use 'E' instead of 'S'
	::openlog(SubsystemID[fLogData.fSubsysID].c_str(), 0|LOG_PID, fFacility);
	::syslog(LOG_ERR, format(msg, 'E').c_str());
	::closelog();
}

void MessageLog::logCriticalMessage(const Message& msg)
{
	::openlog(SubsystemID[fLogData.fSubsysID].c_str(), 0|LOG_PID, fFacility);
	::syslog(LOG_CRIT, format(msg, 'C').c_str());
	::closelog();
}

void logDML(unsigned sessionId, unsigned txnId, const string& statement, const string& owner) 
{ 
	logging::Message::Args args;

	unsigned subsystemId = 20; // DMLProc
	unsigned threadId = 0; // 0 for now

	logging::LoggingID loggingId(subsystemId, sessionId, txnId, threadId);
	logging::MessageLog messageLog(loggingId, LOG_LOCAL2);
	logging::Message m(M0017);
	args.add("|" + owner + "|" + statement);
	m.format(args);
	messageLog.logCriticalMessage(m);
} 

void logDDL(unsigned sessionId, unsigned txnId, const string& statement, const string& owner) 
{
	logging::Message::Args args;

	unsigned subsystemId = 15; // DDLProc
	unsigned threadId = 0; // 0 for now

	logging::LoggingID loggingId(subsystemId, sessionId, txnId, threadId);
	logging::MessageLog messageLog(loggingId, LOG_LOCAL2);
	logging::Message m(M0018);
	args.add("|" + owner + "|" + statement);
	m.format(args);
	messageLog.logCriticalMessage(m);
}

void logCommand(unsigned sessionId, unsigned txnId, const string& statement) {
	logging::Message::Args args;

	unsigned subsystemId = 20; // DMLProc
	unsigned threadId = 0; // 0 for now

	logging::LoggingID loggingId(subsystemId, sessionId, txnId, threadId);
	logging::MessageLog messageLog(loggingId, LOG_LOCAL2);
	logging::Message m(M0019);
	args.add("|" + statement);
	m.format(args);
	messageLog.logCriticalMessage(m);
}

void logEventToDataLog(unsigned messageId, const string& messageText) {
	logging::Message::Args args;

	unsigned subsystemId = 20; // DMLProc
	unsigned threadId = 0; // 0 for now

	logging::LoggingID loggingId(subsystemId, 0, 0, threadId);
	logging::MessageLog messageLog(loggingId, LOG_LOCAL2);
	logging::Message m(messageId);
	args.add(messageText);
	m.format(args);
	messageLog.logCriticalMessage(m);
}

} //namespace logging
