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

#ifndef IDBLOGGER_H_
#define IDBLOGGER_H_

#include <string>
#include "IDBDataFile.h"
#include "IDBFileSystem.h"
#include "logger.h"

namespace idbdatafile
{

class IDBLogger
{
public:
	static void enable(bool enabled);
	static bool isEnabled();

	static void logOpen(IDBDataFile::Types type, const char* fname, const char* mode, unsigned opts, IDBDataFile* ret);
	static void logNoArg(const std::string& fname, const IDBDataFile* ptr, const char* op, int ret);
	static void logRW(const char* op, const std::string& fname, const IDBDataFile* ptr, size_t offset, size_t count, size_t bytesRead);
	static void logSeek(const std::string& fname, const IDBDataFile* ptr, off64_t offset, int whence, int ret);
	static void logTruncate(const std::string& fname, const IDBDataFile* ptr, off64_t length, int ret);
	static void logSize(const std::string& fname, const IDBDataFile* ptr, long long ret);

	static void logFSop(IDBFileSystem::Types type, const char* op, const char *pathname, const IDBFileSystem* ptr, long long ret);
	static void logFSop2(IDBFileSystem::Types type, const char* op, const char *oldpath, const char* newpath, const IDBFileSystem* ptr, long long ret);

    static void syslog(const std::string& msg, logging::LOG_TYPE level);

private:
	static void	writeLog( const std::string& logmsg );

	// prevent anyone from instantiating
	IDBLogger();

	static std::string get_backtrace (int to_skip, int num_to_show);

	static bool	s_enabled;
};

inline
void IDBLogger::enable(bool enabled)
{
	s_enabled = enabled;
}

inline
bool IDBLogger::isEnabled()
{
	return s_enabled;
}

}

#endif /* IDBLOGGER_H_ */
