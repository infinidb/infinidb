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

#include <stdio.h>
#include <string>
#include <sys/stat.h>
#include <sstream>
#include <iostream>
#ifdef _MSC_VER
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <io.h>
#include <windows.h>
#endif
#include "utility.h"
#include "BufferedFile.h"
#include "IDBLogger.h"
using namespace std;

namespace idbdatafile
{

BufferedFile::BufferedFile(const char* fname, const char* mode, unsigned opts) :
	IDBDataFile( fname ),
	m_fp( 0 ),
	m_buffer( 0 )
{
	m_fp = fopen(fname, mode);
	if( m_fp == NULL )
	{
		throw std::runtime_error("unable to open Buffered file ");
	}

	applyOptions( opts );
}

void	BufferedFile::applyOptions( unsigned opts )
{
	if( opts & IDBDataFile::USE_VBUF )
	{
		const int DEFAULT_BUFSIZ = 1*1024*1024;
		m_buffer = new char[DEFAULT_BUFSIZ];
		setvbuf(m_fp, m_buffer, _IOFBF, DEFAULT_BUFSIZ);
	}
	else if( opts & IDBDataFile::USE_NOVBUF )
	{
		setvbuf(m_fp, NULL, _IONBF, 0);
	}
}

BufferedFile::~BufferedFile()
{
	close();
	m_fp = 0;
	delete [] m_buffer;
}

ssize_t BufferedFile::pread(void *ptr, off64_t offset, size_t count)
{
	ssize_t ret = 0;
	int savedErrno;
	ssize_t curpos = tell();

    seek(offset, SEEK_SET);
	ret = read(ptr, count);
	savedErrno = errno;
	seek(curpos, SEEK_SET);

	if( IDBLogger::isEnabled() )
		IDBLogger::logRW("pread", m_fname, this, offset, count, ret);

	errno = savedErrno;
	return ret;
}

ssize_t BufferedFile::read(void *ptr, size_t count)
{
	ssize_t ret = 0;
	ssize_t offset = tell();
	int savedErrno = -1;
	size_t progress = 0;
	uint8_t *ptr8 = (uint8_t *) ptr;

	while (progress < count) {
		ret = fread(ptr8 + progress, 1, count - progress, m_fp);
		savedErrno = errno;
		if (ret <= 0) {
			if (ferror(m_fp)) {
				errno = savedErrno;
				return -1;
			}
			else if (feof(m_fp))
				return progress;
		}
		progress += ret;
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logRW("read", m_fname, this, offset, count, progress);

	errno = savedErrno;
	return progress;
}

ssize_t BufferedFile::write(const void *ptr, size_t count)
{
	ssize_t ret = 0;
	off64_t offset = tell();
	int savedErrno = 0;
	size_t progress = 0;
	uint8_t *ptr8 = (uint8_t *) ptr;

	while (progress < count) {
		ret = fwrite(ptr8 + progress, 1, count - progress, m_fp);
		savedErrno = errno;
		if (ret <= 0 && ferror(m_fp)) {
			errno = savedErrno;
			return -1;
		}
		else if (ret > 0)
            progress += ret;
        // can fwrite() continually return 0 with no error?
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logRW("write", m_fname, this, offset, count, progress);

	errno = savedErrno;
	return progress;
}

int BufferedFile::seek(off64_t offset, int whence)
{
	int ret = 0;
	int savedErrno;
#ifdef _MSC_VER
	ret = _fseeki64(m_fp, offset, whence);
#else
	ret = fseek(m_fp, offset, whence);
#endif
	savedErrno = errno;

	if( IDBLogger::isEnabled() )
		IDBLogger::logSeek(m_fname, this, offset, whence, ret);

	errno = savedErrno;
	return ret;
}

int BufferedFile::truncate(off64_t length)
{
	int ret = 0;
	int savedErrno;

#ifdef _MSC_VER
	ret = _chsize_s(_fileno(m_fp), length);
#else
	ret = ftruncate(fileno(m_fp),length);
#endif
	savedErrno = errno;

	if( IDBLogger::isEnabled() )
		IDBLogger::logTruncate(m_fname, this, length, ret);

	errno = savedErrno;
	return ret;
}

off64_t BufferedFile::size()
{
#ifdef _MSC_VER
	return _filelengthi64(fileno(m_fp));    // Interestingly, implemented as fseek/ftell in the windows crt
#else
	// going to calculate size 2 ways - first, via seek
	off64_t length = -1;
	off64_t here;

	flockfile(m_fp);
	try
	{
		if ((here = ftell(m_fp)) > -1)
		{
			if (fseek(m_fp, 0, SEEK_END) > -1)
			{
				length = ftell(m_fp);
				fseek(m_fp, here, SEEK_SET);
			}
		}
		funlockfile(m_fp);
	}
	catch(...)
	{
		funlockfile(m_fp);
	}
	return length;
#endif
}

off64_t BufferedFile::tell()
{
#ifdef _MSC_VER
	return _ftelli64(m_fp);
#else
	return ftell(m_fp);
#endif
}

int BufferedFile::flush()
{
	int rc = fflush(m_fp);
	int savedErrno = errno;

	if( rc == 0 ) {
#ifdef _MSC_VER
		rc = _commit(_fileno(m_fp));
#else
		rc = fsync( fileno( m_fp ) );
#endif
		savedErrno = errno;
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logNoArg(m_fname, this, "flush", rc);

	errno = savedErrno;
	return rc;
}

time_t BufferedFile::mtime()
{
	time_t ret = 0;
	struct stat statbuf;
	if (::fstat(fileno(m_fp), &statbuf) == 0)
		ret = statbuf.st_mtime;
	else
		ret = (time_t) -1;
	return ret;
}

int BufferedFile::close()
{
	int ret = fclose(m_fp);
	int savedErrno = errno;

	if( IDBLogger::isEnabled() )
		IDBLogger::logNoArg(m_fname, this, "close", ret);

	errno = savedErrno;
	return ret;
}

}
