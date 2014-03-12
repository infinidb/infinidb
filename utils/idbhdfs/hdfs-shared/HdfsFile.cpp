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
 * InfiniDB FOSS License Exception
 * We want free and open source software applications under certain
 * licenses to be able to use the GPL-licensed InfiniDB idbhdfs
 * libraries despite the fact that not all such FOSS licenses are
 * compatible with version 2 of the GNU General Public License.  
 * Therefore there are special exceptions to the terms and conditions 
 * of the GPLv2 as applied to idbhdfs libraries, which are 
 * identified and described in more detail in the FOSS License 
 * Exception in the file utils/idbhdfs/FOSS-EXCEPTION.txt
 */

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
	int savedErrno;

	m_flags = modeStrToFlags(mode);
	if( m_flags == -1 )
	{
		ostringstream oss;
		oss << "Error opening file " << fname << " - unsupported mode " << mode;
		throw std::runtime_error(oss.str());
	}

	m_fs = HdfsFsCache::fs();

	// @bug5476, HDFS do not support O_CREAT|O_APPEND as of 2.0
	// special handle for O_APPEND
	if ((m_flags & O_APPEND) && (hdfsExists(m_fs, fname) != 0))
		m_flags &= ~O_APPEND;

	m_file = hdfsOpenFile(m_fs, fname, m_flags, 0, 0, 0);
	savedErrno = errno;

	if(!m_file)
	{
		ostringstream oss;
		oss << "Error opening file " << fname << ": " << strerror(savedErrno);
		throw std::runtime_error(oss.str());
	}
}

HdfsFile::~HdfsFile()
{
	if( m_file != 0 )
		close();
	m_file = 0;
}

int HdfsFile::reopen()
{
	// if we are trying to repoen, something has already happened
	// (case we know of is a stale read handle after file rewrite)
	// so don't check the return value of close()
	close();

	m_file = hdfsOpenFile(m_fs, m_fname.c_str(), m_flags, 0, 0, 0);
	int savedErrno = errno;

	if( IDBLogger::isEnabled() )
		IDBLogger::logNoArg(m_fname, this, "reopen", m_file != NULL);

	errno = savedErrno;
	return (m_file != NULL ? 0 : -1);
}

ssize_t HdfsFile::real_pread(void *ptr, off64_t offset, size_t count)
{
	int savedErrno = -1;
	size_t progress = 0;
	uint8_t *ptr8 = (uint8_t *) ptr;
	tSize ret = 0;

	// this is an observed case when trying to read from an open file handle
	// that is now stale (because of rewrite/whatever).  If we see this,
	// assume that the file has likely been rewritten and a subsequent attempt
	// to read will be successful if we reopen the file.

	/* Revised s.t. progress is not counted against the number of retry
	attempts. */
	tSize retryCount = 0;
	while (progress < count && retryCount < 100)
	{
		ret = hdfsPread(m_fs, m_file, offset + progress, &ptr8[progress], count - progress);
		savedErrno = errno;
		if (ret > 0) {    // making progress, reset the retry counter
			retryCount = 0;
			progress += ret;
		}
		else {  // an error or EOF
//			cout << "retrying... progress = " << progress << " count = " << count <<
//				" filename = " << name() << endl;
			if (retryCount < 10)
				usleep(1000);
			else
				usleep(200000);
			reopen();   // keep retrying, regardless of an error on reopen
			retryCount++;
		}
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logRW("pread", m_fname, this, offset, count, ret);

	errno = savedErrno;
	return progress;
}

ssize_t HdfsFile::pread(void *ptr, off64_t offset, size_t count)
{
	boost::mutex::scoped_lock lock(m_mutex);
	return real_pread(ptr, offset, count);
}

ssize_t HdfsFile::read(void *ptr, size_t count)
{
	boost::mutex::scoped_lock lock(m_mutex);
	int savedErrno;

	tOffset offset = tell();
	if (offset < 0)
		return offset;
	/* May get a performance boost by implementing read() s.t. it
	doesn't require a seek afterward, but probably not though. */
	ssize_t numRead = real_pread(ptr, offset, count);
	savedErrno = errno;
	if (numRead > 0)
		hdfsSeek(m_fs, m_file, offset + numRead);
	errno = savedErrno;
	return numRead;
}

ssize_t HdfsFile::write(const void *ptr, size_t count)
{
	ssize_t offset = tell();
	size_t progress = 0;
	uint8_t *ptr8 = (uint8_t *) ptr;
	uint32_t zeroByteCounter = 0;
	int savedErrno;
	tSize ret;

	/* Rewrote the write() fcn to not consider minor progress an error.  As
	long as the write is making forward progress, it will try "forever".  If
	it stalls completely for 100 attempts (~= 20s) in a row, it will return the
	partial write bytecount. */
	while (progress < count && zeroByteCounter < 100) {
		ret = hdfsWrite(m_fs, m_file, &ptr8[progress], count - progress);
		savedErrno = errno;
		if (ret < 0 && errno != EINTR) {
			ostringstream oss;
			oss << "hdfsWrite set errno=" << errno
					<< ", \"" << strerror(errno);
			// TODO-this really needs to be syslog'ed to aid in tblshooting
			if( IDBLogger::isEnabled() )
				IDBLogger::logNoArg(oss.str(), this, "write", ret);
			errno = savedErrno;
			return ret;
		}
		else if (ret == 0) {  // not making progress, might be a problem.
			zeroByteCounter++;
			if (zeroByteCounter < 10)    // a back-off timer
				usleep(1000);
			else
				usleep(200000);
		}
		else if (ret > 0) {   // making progress, keep going.
			zeroByteCounter = 0;
			progress += ret;
		}
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logRW("write", m_fname, this, offset, count, progress);

	return progress;
}

int HdfsFile::seek(off64_t offset, int whence)
{
	boost::mutex::scoped_lock lock(m_mutex);
	int savedErrno;
	off_t mod_offset = offset;
	if( whence == SEEK_CUR )
	{
		mod_offset = mod_offset + tell();
	} else if( whence == SEEK_END )
	{
		mod_offset = mod_offset + size();
	}
	int ret = hdfsSeek(m_fs, m_file, mod_offset);
	savedErrno = errno;

	if( IDBLogger::isEnabled() )
		IDBLogger::logSeek(m_fname, this, offset, whence, ret);

	errno = savedErrno;
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
	int savedErrno;

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
	savedErrno = errno;	

	if( IDBLogger::isEnabled() )
		IDBLogger::logSize(m_fname, this, ret);

	errno = savedErrno;
	return ret;
}

off64_t HdfsFile::tell()
{
	return hdfsTell(m_fs, m_file);
}

int HdfsFile::flush()
{
	int ret = hdfsFlush(m_fs, m_file);
	int savedErrno = errno;

	if( IDBLogger::isEnabled() )
		IDBLogger::logNoArg(m_fname, this, "flush", ret);

	errno = savedErrno;
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

int HdfsFile::close()
{
	int ret = 0;
	int savedErrno = EINVAL;  // corresponds to m_file == 0
	if( m_file != 0 )
	{
		ret = hdfsCloseFile(m_fs, m_file);
		savedErrno = errno;
		m_file = 0;
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logNoArg(m_fname, this, "close", ret);
	errno = savedErrno;
	return ret;
}

}
