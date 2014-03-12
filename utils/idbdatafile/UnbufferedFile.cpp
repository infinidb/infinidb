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

#include "UnbufferedFile.h"
#include "IDBLogger.h"
#include "utility.h"

#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <sstream>

using namespace std;

namespace idbdatafile
{

UnbufferedFile::UnbufferedFile(const char* fname, const char* mode, unsigned opts) :
	IDBDataFile( fname )
{
#ifdef _MSC_VER
	int createFlags;
	int flags = modeStrToFlags(mode, createFlags);
	m_fd = CreateFile(fname, flags, FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE, 0,
		createFlags, FILE_ATTRIBUTE_NORMAL|FILE_FLAG_NO_BUFFERING, 0);
	if( m_fd == INVALID_HANDLE_VALUE )
	{
		throw std::runtime_error("unable to open Unbuffered file ");
	}
#else
	int flags = modeStrToFlags(mode);
	if( flags == -1 )
	{
		ostringstream oss;
		oss << "Error opening file - unsupported mode " << mode;
		throw std::runtime_error(oss.str());
	}
	// special flags we set by convention due to InfiniDB usage
	flags |= O_LARGEFILE|O_NOATIME;

	if( opts & IDBDataFile::USE_ODIRECT )
	{
		flags |= O_DIRECT;
	}

	m_fd = ::open(fname, flags, S_IRWXU);
	if( m_fd == -1 )
	{
		m_fd = INVALID_HANDLE_VALUE;
		throw std::runtime_error("unable to open Unbuffered file ");
	}
#endif
}

UnbufferedFile::~UnbufferedFile()
{
	close();
}

ssize_t UnbufferedFile::pread(void *ptr, off64_t offset, size_t count)
{
	ssize_t ret;
	int savedErrno;

	if (m_fd == INVALID_HANDLE_VALUE)
		return -1;
#ifdef _MSC_VER
	OVERLAPPED ovl;
	memset(&ovl, 0, sizeof(ovl));
	DWORD mtest = (DWORD)offset;
	ovl.Offset = (DWORD)(offset & 0x00000000FFFFFFFF);
	ovl.OffsetHigh = (DWORD)(offset >> 32);
	DWORD bytesRead;
	if (ReadFile(m_fd, ptr, (DWORD)count, &bytesRead, &ovl))
		ret = bytesRead;
	else
		ret = -1;
	savedErrno = errno;
#else
	ret = ::pread(m_fd, ptr, count, offset);
	savedErrno = errno;
#endif
	if( IDBLogger::isEnabled() )
		IDBLogger::logRW("pread", m_fname, this, offset, count, ret);

	errno = savedErrno;
	return ret;
}

ssize_t UnbufferedFile::read(void *ptr, size_t count)
{
	ssize_t ret = 0;
	ssize_t offset = tell();
	int savedErrno;

#ifdef _MSC_VER
	DWORD bytesRead;
	if (ReadFile(m_fd, ptr, (DWORD)count, &bytesRead, NULL))
		ret = bytesRead;
	else
		ret = -1;
#else
	ret = ::read(m_fd,ptr,count);
#endif
	savedErrno = errno;

	if( IDBLogger::isEnabled() )
		IDBLogger::logRW("read", m_fname, this, offset, count, ret);

	errno = savedErrno;
	return ret;
}

ssize_t UnbufferedFile::write(const void *ptr, size_t count)
{
	ssize_t ret = 0;
	ssize_t offset = tell();
	int savedErrno;

#ifdef _MSC_VER
	DWORD bytesWritten;
	if (WriteFile(m_fd, ptr, (DWORD)count, &bytesWritten, NULL))
		ret = bytesWritten;
	else
		ret = -1;
#else
	ret = ::write(m_fd, ptr, count);
#endif
	savedErrno = errno;

	if( IDBLogger::isEnabled() )
		IDBLogger::logRW("write", m_fname, this, offset, count, ret);

	errno = savedErrno;
	return ret;
}

int UnbufferedFile::seek(off64_t offset, int whence)
{
	int ret;
	int savedErrno;

#ifdef _MSC_VER
	LONG lDistanceToMove = LONG(offset & 0x00000000FFFFFFFF);
	LONG lDistanceToMoveHigh = LONG(offset >> 32);
	ret = SetFilePointer(m_fd, lDistanceToMove, &lDistanceToMoveHigh, whence);
#else
	ret = (lseek(m_fd, offset, whence) >= 0) ? 0 : -1;
#endif
	savedErrno = errno;

	if( IDBLogger::isEnabled() )
		IDBLogger::logSeek(m_fname, this, offset, whence, ret);

	errno = savedErrno;
	return ret;
}

int UnbufferedFile::truncate(off64_t length)
{
	int ret;
	int savedErrno;

#ifdef _MSC_VER
	LONG lDistanceToMove = LONG(length & 0x00000000FFFFFFFF);
	LONG lDistanceToMoveHigh = LONG(length >> 32);
	ret = SetFilePointer(m_fd, lDistanceToMove, &lDistanceToMoveHigh, SEEK_SET);
	if (ret > 0)
		ret = SetEndOfFile(m_fd);
#else
	ret = ftruncate(m_fd,length);
#endif
	savedErrno = errno;

	if( IDBLogger::isEnabled() )
		IDBLogger::logTruncate(m_fname, this, length, ret);

	errno = savedErrno;
	return ret;
}

off64_t UnbufferedFile::size()
{
	off64_t ret = 0;
	int savedErrno;

#ifdef _MSC_VER
	DWORD hi = 0;
	DWORD lo = GetFileSize(m_fd, &hi);
	ret = off64_t(((uint64_t)hi) << 32) | lo;
#else
	struct stat statBuf;
	int rc = ::fstat( m_fd, &statBuf );
	ret = ((rc == 0) ? statBuf.st_size : -1);
#endif
	savedErrno = errno;

	if( IDBLogger::isEnabled() )
		IDBLogger::logSize(m_fname, this, ret);

	errno = savedErrno;
	return ret;
}

off64_t UnbufferedFile::tell()
{
	off64_t ret;
#ifdef _MSC_VER
	LARGE_INTEGER wRet;
	LARGE_INTEGER dist;
	dist.QuadPart = 0;
	SetFilePointerEx(m_fd, dist, &wRet, FILE_CURRENT);
	ret = wRet.QuadPart;
#else
	ret = lseek(m_fd, 0, SEEK_CUR);
#endif
	return ret;
}

int UnbufferedFile::flush()
{
	int ret;
	int savedErrno;

#ifdef _MSC_VER
	ret = FlushFileBuffers(m_fd);
	// In this case for Windows, ret is the reverse of Linux
	if (ret == 0)
		ret = -1;
	else
		ret = 0;
#else
	ret = fsync( m_fd );
#endif
	savedErrno = errno;

	if( IDBLogger::isEnabled() )
		IDBLogger::logNoArg(m_fname, this, "flush", ret);

	errno = savedErrno;
	return ret;
}

time_t UnbufferedFile::mtime()
{
	time_t ret = 0;
#ifdef _MSC_VER
	BY_HANDLE_FILE_INFORMATION info;
	if (GetFileInformationByHandle(m_fd, &info))
	{
		ret = time_t((uint64_t)info.ftLastAccessTime.dwHighDateTime << 32) | info.ftLastAccessTime.dwLowDateTime;
	}
	else
	{
		ret = (time_t) -1;
	}
#else
	struct stat statbuf;
	if (::fstat(m_fd, &statbuf) == 0)
		ret = statbuf.st_mtime;
	else
		ret = (time_t) -1;
#endif
	return ret;
}

int UnbufferedFile::close()
{
	int ret = -1;
	int savedErrno = EINVAL;  // corresponds to INVALID_HANDLE_VALUE

	if (m_fd != INVALID_HANDLE_VALUE)
	{
#ifdef _MSC_VER
		ret = CloseHandle(m_fd);
		// In this case for Windows, ret is the reverse of Linux
		if (ret == 0)
			ret = -1;
		else
			ret = 0;
#else
		ret = ::close(m_fd);
#endif
		savedErrno = errno;
	}
	if( IDBLogger::isEnabled() )
		IDBLogger::logNoArg(m_fname, this, "close", ret);
	errno = savedErrno;
	return ret;
}

}
