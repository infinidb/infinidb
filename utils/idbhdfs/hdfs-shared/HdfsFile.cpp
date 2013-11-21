/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#include "HdfsFile.h"

#include "HdfsFsCache.h"
#include "IDBLogger.h"
#include "utility.h"

#include <fcntl.h>
#include <string>
#include <iostream>
#include <sstream>
#include <errno.h>
#include <string.h>

using namespace std;

namespace idbdatafile
{

HdfsFile::HdfsFile(const char* fname, const char* mode, unsigned /* opts */) :
	IDBDataFile( fname ),
	m_file(0),
	m_fs(0)
{
	m_flags = modeStrToFlags(mode);
	if( m_flags == -1 )
	{
		ostringstream oss;
		oss << "Error opening file - unsupported mode " << mode;
    	throw std::runtime_error(oss.str());
	}

	m_fs = HdfsFsCache::fs();

	// @bug5476, HDFS do not support O_CREAT|O_APPEND as of 2.0
	// special handle for O_APPEND
	if ((m_flags & O_APPEND) && (hdfsExists(m_fs, fname) != 0))
		m_flags &= ~O_APPEND;

    m_file = hdfsOpenFile(m_fs, fname, m_flags, 0, 0, 0);

    if(!m_file)
    {
    	throw std::runtime_error("unable to open HDFS file ");
    }
}

HdfsFile::~HdfsFile()
{
	if( m_file != 0 )
		close();
	m_file = 0;
}

void HdfsFile::reopen() throw (std::exception)
{
	// use of this function is not threadsafe, but so far don't have
	// a better option for handling the stale file problem in PrimProc.
	// For now we will live with potential problems up to and including a
	// PrimProc crash if it happens that at least one thread is in
	// here while another is active.

	// if we are trying to repoen, something has already happened
	// (case we know of is a stale read handle after file rewrite)
	// so don't check the return value of close()
	close();

	m_file = hdfsOpenFile(m_fs, m_fname.c_str(), m_flags, 0, 0, 0);

	if( IDBLogger::isEnabled() )
		IDBLogger::logNoArg(m_fname, this, "reopen", m_file != NULL);

	if( m_file == NULL )
	{
		throw std::runtime_error("failed to reopen HDFS file!");
	}
}

ssize_t HdfsFile::pread(void *ptr, off64_t offset, size_t count)
{
	boost::mutex::scoped_lock lock(m_mutex);

	tSize ret = hdfsPread(m_fs, m_file, offset, ptr, count);
	// this is an observed case when trying to read from an open file handle
	// that is now stale (because of rewrite/whatever).  If we see this,
	// assume that the file has likely been rewritten and a subsequent attempt
	// to read will be successful if we reopen the file.

	tSize retryCount = 0;
	while (ret != (tSize) count)
	{
		//if (ret != count)
		//	IDBLogger::logRW("pread_retry", m_fname, this, offset, count, ret);
		if (retryCount >= 50)
			break;
		usleep(1000);
		reopen();
		ret = hdfsPread(m_fs, m_file, offset, ptr, count);
		retryCount++;
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logRW("pread", m_fname, this, offset, count, ret);

	return ret;
}

ssize_t HdfsFile::read(void *ptr, size_t count)
{
	size_t offset = tell();
	tSize ret = hdfsRead(m_fs, m_file, ptr, count);
	// see comment in pread
	if( ret < 0 )
	{
		reopen();
		if (hdfsSeek(m_fs, m_file, offset) == 0)
			ret = hdfsRead(m_fs, m_file, ptr, count);
	}
	else if ((size_t) ret < count)  // ret >= 0
	{
		// retry
		int retryCount = 0;
		while (ret >= 0 && ret != (tSize) count && ++retryCount < 40)
		{
			ssize_t n = hdfsRead(m_fs, m_file, (char*)ptr+ret, count-ret);
			if (n >= 0)
				ret += n;
			else
				ret = n;
		}
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logRW("read", m_fname, this, offset, count, ret);

	return ret;
}

ssize_t HdfsFile::write(const void *ptr, size_t count)
{
	ssize_t offset = tell();
	tSize ret = hdfsWrite(m_fs, m_file, ptr, count);
	if( ret < 0 )
	{
		ostringstream oss;
		oss << "hdfsWrite set errno=" << errno
				<< ", \"" << strerror(errno);
		// TODO-this really needs to be syslog'ed to aid in tblshooting
		if( IDBLogger::isEnabled() )
			IDBLogger::logNoArg(oss.str(), this, "write", ret);
	}
	else if ((size_t) ret < count)  // ret >= 0
	{
		// retry once for now
		ret += hdfsWrite(m_fs, m_file, (const char*)ptr+ret, count-ret);
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logRW("write", m_fname, this, offset, count, ret);

	return ret;
}

int HdfsFile::seek(off64_t offset, int whence)
{
	off_t mod_offset = offset;
	if( whence == SEEK_CUR )
	{
		mod_offset = mod_offset + tell();
	} else if( whence == SEEK_END )
	{
		mod_offset = mod_offset + size();
	}
	int ret = hdfsSeek(m_fs, m_file, mod_offset);

	if( IDBLogger::isEnabled() )
		IDBLogger::logSeek(m_fname, this, offset, whence, ret);

	return ret;
}

int HdfsFile::truncate(off64_t length)
{
	// no truncate operation in HDFS
	if( IDBLogger::isEnabled() )
		IDBLogger::logTruncate(m_fname, this, length, -1);

	return -1;
}

off64_t HdfsFile::size()
{
	off64_t ret = 0;
	if( ( m_flags & O_RDONLY ) != 0 )
	{
		hdfsFileInfo* fileinfo;
		fileinfo = hdfsGetPathInfo(m_fs,m_fname.c_str());
		ret = (fileinfo ? fileinfo->mSize : -1);
		if( fileinfo )
			hdfsFreeFileInfo(fileinfo,1);
	}
	else
	{
		// if file is open for either WRITE or APPEND then we know that
		// size is always the current file offset since HDFS can only
		// write at the end
		ret = tell();
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logSize(m_fname, this, ret);

    return ret;
}

off64_t HdfsFile::tell()
{
	return hdfsTell(m_fs, m_file);
}

int HdfsFile::flush()
{
	int ret = hdfsFlush(m_fs, m_file);

	if( IDBLogger::isEnabled() )
		IDBLogger::logNoArg(m_fname, this, "flush", ret);

	return ret;
}

time_t HdfsFile::mtime()
{
	boost::mutex::scoped_lock lock(m_mutex);

	time_t ret = 0;
	hdfsFileInfo* fileinfo;
	fileinfo = hdfsGetPathInfo(m_fs,m_fname.c_str());
	ret = (fileinfo ? fileinfo->mLastMod : -1);
	if( fileinfo )
		hdfsFreeFileInfo(fileinfo,1);

	return ret;
}

void HdfsFile::close()
{
	int ret = 0;
	if( m_file != 0 )
	{
		ret = hdfsCloseFile(m_fs, m_file);
		m_file = 0;
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logNoArg(m_fname, this, "close", ret);
}

}
