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

#ifndef SKIP_UNWIND
#define SKIP_UNWIND
#endif

#include <unistd.h>
#if !defined(_MSC_VER) && !defined(SKIP_UNWIND)
#include <libunwind.h>
#include <demangle.h>
#endif
#define basename

#include "IDBLogger.h"

#include <sstream>
#include <fstream>
#include <time.h>
#include <sys/time.h>
using namespace std;

using namespace logging;

namespace idbdatafile
{

bool IDBLogger::s_enabled = false;

/*
 * Log format is:
 * [timestamp] , [filename/pathname] , [ptr] , [operation] , [op-args1] , [op-args2] , [ret] , [backtrace]
 */
void IDBLogger::writeLog( const std::string& logmsg )
{
#ifdef _MSC_VER
    DWORD pid = GetCurrentProcessId();
    DWORD threadid = GetCurrentThreadId();
#else
	pid_t pid = getpid();
	pthread_t threadid = pthread_self();
#endif
	ostringstream fname;
	fname << "/tmp/idbdf-log-" << pid << "-" << threadid << ".csv";

	ofstream output;
	output.open( fname.str().c_str(), ios::out | ios::app );

    char            fmt[64], buf[64];
    struct timeval  tv;
    struct tm       tm;

    gettimeofday(&tv, NULL);
#ifdef _MSC_VER
	errno_t p = 0;
	time_t t = (time_t)tv.tv_sec;
	p = localtime_s(&tm, &t);
	if (p != 0)
		memset(&tm, 0, sizeof(tm));
#else
    localtime_r(&tv.tv_sec, &tm);
#endif
    strftime(fmt, sizeof fmt, "\'%Y-%m-%d %H:%M:%S.%%06u", &tm);
    snprintf(buf, sizeof buf, fmt, tv.tv_usec);

	output << buf << "," << logmsg << "," << get_backtrace(3,4) << endl;
	output.close();
}

void IDBLogger::logOpen(IDBDataFile::Types type, const char* fname, const char* mode, unsigned opts, IDBDataFile* ret)
{
	ostringstream logmsg;
	logmsg << fname << ",,open,type=";
	logmsg << (( type == IDBDataFile::UNBUFFERED ) ? "unbuffered" : ( type == IDBDataFile::HDFS ) ? "hdfs" : "buffered" );
	logmsg << ";mode=" << mode;
	logmsg << ";opts=" << opts << ",,";
	logmsg << ret;
	writeLog( logmsg.str() );
}

void IDBLogger::logNoArg(const std::string& fname, const IDBDataFile* ptr, const char* op, int ret)
{
	ostringstream logmsg;
	logmsg << fname << "," << ptr << "," << op << ",,," << ret;
	writeLog( logmsg.str() );
}

void IDBLogger::logRW(const char* op, const std::string& fname, const IDBDataFile* ptr, size_t offset, size_t count, size_t bytesRead)
{
	ostringstream logmsg;
	logmsg << fname << "," << ptr << "," << op << "," << offset << "," << count << "," << bytesRead;
	writeLog( logmsg.str() );
}

void IDBLogger::logSeek(const std::string& fname, const IDBDataFile* ptr, off64_t offset, int whence, int ret)
{
	ostringstream logmsg;
	logmsg << fname << "," << ptr << ",seek," << offset << "," << whence << "," << ret;
	writeLog( logmsg.str() );
}

void IDBLogger::logTruncate(const std::string& fname, const IDBDataFile* ptr, off64_t length, int ret)
{
	ostringstream logmsg;
	logmsg << fname << "," << ptr << ",truncate," << length << ",," << ret;
	writeLog( logmsg.str() );
}

void IDBLogger::logSize(const std::string& fname, const IDBDataFile* ptr, long long ret)
{
	ostringstream logmsg;
	logmsg << fname << "," << ptr << ",size,,," << ret;
	writeLog( logmsg.str() );
}

void IDBLogger::logFSop(IDBFileSystem::Types type, const char* op, const char *pathname, const IDBFileSystem* ptr, long long ret)
{
	ostringstream logmsg;
	logmsg << pathname << "," << ptr << "," << op << ",";
	logmsg << "type=" << ( type == IDBFileSystem::POSIX ? "posix":"hdfs" ) << ",";
	logmsg << "," << ret;
	writeLog( logmsg.str() );
}

void IDBLogger::logFSop2(IDBFileSystem::Types type, const char* op, const char *oldpath, const char* newpath, const IDBFileSystem* ptr, long long ret)
{
	ostringstream logmsg;
	logmsg << oldpath << "," << ptr << "," << op << ",";
	logmsg << "type=" << ( type == IDBFileSystem::POSIX ? "posix":"hdfs" ) << ",";
	logmsg << newpath;
	logmsg << "," << ret;
	writeLog( logmsg.str() );
}

string IDBLogger::get_backtrace (int to_skip, int num_to_show)
{
	string retval;
#if !defined(_MSC_VER) && !defined(SKIP_UNWIND)
	char name[256];
	unw_cursor_t cursor; unw_context_t uc;
	unw_word_t ip, sp, offp;

	unw_getcontext (&uc);
	unw_init_local (&cursor, &uc);

	int frame_ct = 0;
	while (unw_step(&cursor) > 0 && frame_ct < (to_skip + num_to_show))
	{
		++frame_ct;
		if( frame_ct > to_skip )
		{
			name[0] = '\0';
			unw_get_proc_name (&cursor, name, 256, &offp);
			unw_get_reg (&cursor, UNW_REG_IP, &ip);
			unw_get_reg (&cursor, UNW_REG_SP, &sp);

			if( retval.length() )
				retval = retval + ",";

			char* dem_name_ptr = cplus_demangle(name, 0);
//          replace with this for full function prototype names including parms
//			char* dem_name_ptr = cplus_demangle(name,DMGL_PARAMS | DMGL_ANSI);
			if( dem_name_ptr )
				retval = retval + "\"" + dem_name_ptr + "\"";
			else
				retval = retval + "\"" + name + "\"";
		}
	}
#endif
	return retval;
}

void IDBLogger::syslog(const std::string& msg, logging::LOG_TYPE level)
{
	logging::Message::Args args;
	logging::Message message(2);
	args.add(msg);
	message.format(args);
	logging::LoggingID lid(35);
	logging::MessageLog ml(lid);
	switch (level)
	{
		case LOG_TYPE_DEBUG:
			ml.logDebugMessage(message);
			break;
		case LOG_TYPE_INFO:
			ml.logInfoMessage(message);
			break;
		case LOG_TYPE_WARNING:
			ml.logWarningMessage(message);
			break;
		case LOG_TYPE_ERROR:
			ml.logErrorMessage(message);
			break;
		case LOG_TYPE_CRITICAL:
			ml.logCriticalMessage(message);
			break;
	}
}

}
