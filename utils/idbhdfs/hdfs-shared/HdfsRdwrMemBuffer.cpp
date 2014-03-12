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

#include "HdfsRdwrMemBuffer.h"
#include "HdfsRdwrFileBuffer.h"
#include "HdfsFsCache.h"
#include "HdfsFile.h"
#include "IDBPolicy.h"
#include "IDBLogger.h"
#include "MonitorProcMem.h"
#include "atomicops.h"
#include <string>
#include <string.h>
#include <assert.h>
#include <boost/scoped_ptr.hpp>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <stdexcept>

using namespace std;

namespace idbdatafile
{

int64_t  HdfsRdwrMemBuffer::t_totalbuff = 0;

HdfsRdwrMemBuffer::HdfsRdwrMemBuffer(const char* fname, const char* mode, unsigned opts, unsigned colWidth) :
	IDBDataFile(fname),
	m_buffer(0),
	m_cur(0),
	m_last(0),
	m_size(0),
	m_dirty(false),
	m_new(false),
	m_noflush(false),
	m_pFileBuffer(NULL)
{
    m_fColWidth = colWidth;
	IDBFileSystem& fs = IDBFactory::getFs( HDFS );
    off64_t bufSize;
    if (colWidth == 0)
        bufSize = DEFSIZE;
    else if (colWidth > 8)
        bufSize = 8 * EXTENTSIZE;
    else
        bufSize = colWidth * EXTENTSIZE;

	// append an extra tmp extension if USE_TMPFILE extension is set.
	string input(fname);
	if (opts & USE_TMPFILE)
	{
        m_noflush = true;
		m_fname += ".tmp";   // use .tmp as working copy to replace the ".hdr/chk" backup.
		if (IDBPolicy::exists(m_fname.c_str()))
			input = m_fname; // get from .tmp if revisit
		else
			m_new = true;
	}

    if( m_fname.find(".cdf") == m_fname.length() - 4 )
        m_noflush = true;

    // cache the size first - we will use this to determine if the
    // file already exists and needs to be read into the buffer
    off64_t size = fs.size(input.c_str());

	// first we will try to open the file for writing - if this fails
	// it means we can't actually open the file for writing and need
	// to throw exception anyway
	bool truncate = (mode != NULL && *mode == 'w');
	if( size < 0 || truncate )
	{
		HdfsFile* trywrite = new HdfsFile(input.c_str(), "w", 0);
		delete trywrite;

		size = 0;
	}

	// if we get here, then will be ok to write later.  Now we need to
	// open one for reading if the file existed previously
	if( size > 0 )
	{
		HdfsFile tryread(input.c_str(), "r", 0);

        checkRealloc(size);
		if (m_buffer) {

            // HDFS is allowed to read less than the # of bytes we specified on
            // a single call to read() or pread().  We are especially susceptible
            // here in the constructor because we may need to open a large file
            // for rewriting
            ssize_t bytesReadSoFar = 0;
            while( bytesReadSoFar < (ssize_t) size )
            {
                ssize_t bytesread = tryread.pread( m_buffer + bytesReadSoFar, bytesReadSoFar, size - bytesReadSoFar );

                if( bytesread <= 0 )
                {
                    ostringstream oss;
                    oss << "HdfsRdwrMemBuffer: unable to completely load file " << fname << " into memory. only read "
                        << bytesReadSoFar << " bytes, expected " << size;
                    throw std::runtime_error(oss.str());
                }

                bytesReadSoFar += bytesread;
            }
            m_last = size;
        }
	}
	else
	{
		// no file contents so nothing to read.  just initialize default buffer
		allocBuffer(bufSize);
	}
}

HdfsRdwrMemBuffer::~HdfsRdwrMemBuffer()
{
	close();
	releaseBuffer();
}

void HdfsRdwrMemBuffer::allocBuffer(size_t size)
{
    // Keep a running total hdfsrdwr buffer space used
    // Currently, only used for display, but could conceivably be used
    // in some algoritm to determine if/when to use file buffering.
    (void)atomicops::atomicAdd<int64_t>(&t_totalbuff, size-m_size);
    if( IDBLogger::isEnabled() )
    {
        ostringstream oss;
        oss << "Allocate: total hdfsrdwr buffer = " << t_totalbuff;
        IDBLogger::logNoArg(oss.str(), this, "buffer", size-m_size);
    }

	if( m_buffer )
	{
		// this is a reallocate case ; only supports growing at this point
		unsigned char* newbuffer = new unsigned char[size];
		memcpy( newbuffer, m_buffer, m_size);
		delete [] m_buffer;
		m_buffer = newbuffer;
	}
	else
		m_buffer = new unsigned char[size];
	m_size = size;
}

void HdfsRdwrMemBuffer::releaseBuffer()
{
    (void)atomicops::atomicSub<int64_t>(&t_totalbuff, m_size);
    if( IDBLogger::isEnabled() )
    {
        ostringstream oss;
        oss << "Release:  total hdfsrdwr buffer = " << t_totalbuff;
        IDBLogger::logNoArg(oss.str(), this, "buffer", -m_size);
    }

    delete [] m_buffer;
    m_size = 0;
    m_buffer = NULL;
}

ssize_t HdfsRdwrMemBuffer::pread(void *ptr, off64_t offset, size_t count)
{
    // If we've switched to a file buffer
    if (m_pFileBuffer)
    {
        return m_pFileBuffer->pread(ptr, offset, count);
    }

    off64_t start = min( offset, m_last );

	ssize_t bytestoread = (ssize_t)min( (off64_t)count, m_last - start );
	if( bytestoread > 0 )
		memcpy(ptr, &(m_buffer[start]), bytestoread);

	if( IDBLogger::isEnabled() )
		IDBLogger::logRW("pread", m_fname, this, offset, count, bytestoread);

	return bytestoread;
}

ssize_t HdfsRdwrMemBuffer::read(void *ptr, size_t count)
{
    // If we've switched to a file buffer
    if (m_pFileBuffer)
    {
        return m_pFileBuffer->read(ptr, count);
    }
	ssize_t ret = pread(ptr, m_cur, count);
	if (ret > 0)
		m_cur += ret;
	return ret;
}

/* Allocates more mem or switches to filemode, based on (m_cur + count) */
void HdfsRdwrMemBuffer::checkRealloc(size_t count)
{
    if (m_pFileBuffer)   // already in file-backed mode
        return;

	if( m_cur + static_cast<off64_t>( count ) > m_size )
	{
		size_t newsize;
		if (m_size != 0) {
			newsize = m_size * 2;
			while( newsize < (m_cur + count) )
				newsize = newsize * 2;
		}
		else    // first allocation
			newsize = count;

		// If there's enough memory, get some
		if ((IDBPolicy::hdfsRdwrBufferMaxSize() == 0 ||
			(HdfsRdwrMemBuffer::getTotalBuff() + newsize) < IDBPolicy::hdfsRdwrBufferMaxSize())
			&& utils::MonitorProcMem::isMemAvailable(newsize))
        {
            allocBuffer(newsize);
        }
        else
        {
            m_pFileBuffer = new HdfsRdwrFileBuffer(this);
            m_pFileBuffer->seek(m_cur, SEEK_SET);
            releaseBuffer();
        }
	}
}


ssize_t HdfsRdwrMemBuffer::write(const void *ptr, size_t count)
{
	// mark dirty
	m_dirty = true;

    // If we've switched to a file buffer
    if (m_pFileBuffer)
    {
        return m_pFileBuffer->write(ptr, count);
    }

    // cache this for the log below
	size_t offset = m_cur;
	// first see if we need to reallocate
	checkRealloc(count);
	if (m_pFileBuffer)
        return m_pFileBuffer->write(ptr, count);

	memcpy( &(m_buffer[m_cur]), ptr, count );
	m_cur += count;

	// update our end pointer if we just wrote new bytes
	if( m_cur > m_last )
		m_last = m_cur;

	if( IDBLogger::isEnabled() )
		IDBLogger::logRW("write", m_fname, this, offset, count, count);

	return count;
}

int HdfsRdwrMemBuffer::seek(off64_t offset, int whence)
{
	int savedErrno = 0;  // success errno

    // If we've switched to a file buffer
    if (m_pFileBuffer)
    {
        return m_pFileBuffer->seek(offset, whence);
    }

    off64_t mod_offset = offset;
	if( whence == SEEK_CUR )
	{
		mod_offset += m_cur;
	}
	else if( whence == SEEK_END )
	{
		mod_offset += m_last;
	}

	int ret = 0;
	if( mod_offset < 0 )
	{
		// don't change m_cur, just return error
		savedErrno = EINVAL;
		ret = -1;
	}
	else
	{
        checkRealloc(mod_offset - m_cur);
        if (m_pFileBuffer)
            return m_pFileBuffer->seek(offset, whence);
   		// if new offset beyond eof, null out the gap.
        if (mod_offset > m_last) {
            memset(&(m_buffer[m_last]), 0, mod_offset - m_last);
            m_last = mod_offset;
        }
        m_cur = mod_offset;
    }

	if( IDBLogger::isEnabled() )
		IDBLogger::logSeek(m_fname, this, offset, whence, ret);

	errno = savedErrno;
	return ret;
}

int HdfsRdwrMemBuffer::truncate(off64_t length)
{
	int savedErrno = 0;

	// mark dirty
	m_dirty = true;

    // If we've switched to a file buffer
    // opportunity to switch back to mem buffer?
    if (m_pFileBuffer)
    {
        return m_pFileBuffer->truncate(length);
    }

    int ret = 0;
	if( length >= 0 && length <= (off_t) m_last )
	{
		m_last = length;
		if ( m_cur > m_last )
			m_cur = m_last;
	}
	else if (length < 0)
	{
		// nonsensical input
		savedErrno = EINVAL;
		ret = -1;
    }
	else if (length > (off_t) m_last) {
		// truncate(toobig) means extending the file to toobig bytes
        checkRealloc(length - m_cur);
        if (m_pFileBuffer)
            return m_pFileBuffer->truncate(length);
		m_last = length;
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logTruncate(m_fname, this, length, ret);

	errno = savedErrno;
	return ret;
}

off64_t HdfsRdwrMemBuffer::size()
{
    // If we've switched to a file buffer
    if (m_pFileBuffer)
    {
        return m_pFileBuffer->size();
    }

	if( IDBLogger::isEnabled() )
		IDBLogger::logSize(m_fname, this, m_last);

    return m_last;
}

off64_t HdfsRdwrMemBuffer::tell()
{
    // If we've switched to a file buffer
    if (m_pFileBuffer)
    {
        return m_pFileBuffer->tell();
    }

    return m_cur;
}

int HdfsRdwrMemBuffer::flush()
{
	if ( (m_dirty || m_new) && !m_noflush )
	{
		return flushImpl();
	}

	return 0;
}

int HdfsRdwrMemBuffer::flushImpl()
{
	// If we've switched to a file buffer
	if (m_pFileBuffer)
	{
		return m_pFileBuffer->flush();
	}

	IDBDataFile* writeFile = IDBDataFile::open(HDFS, m_fname.c_str(), "w", 0);
	if ( !writeFile )
	{
		ostringstream oss;
		oss << "HdfsRdwrMemBuffer: unable to open file " << m_fname.c_str() << " for writing";
		throw std::runtime_error(oss.str());
	}

	ssize_t ret = writeFile->write(m_buffer, m_last);
	delete writeFile;

	if( static_cast<off64_t>( ret ) != m_last )
	{
		ostringstream oss;
		oss << "HdfsRdwrMemBuffer: unable to write all bytes to hdfs file " << m_fname.c_str() << ". File system full?";
		throw std::runtime_error(oss.str());
	}

	m_dirty = false;
	m_new = false;

	if( IDBLogger::isEnabled() )
		IDBLogger::logNoArg(m_fname, this, "flush", 0);

	return 0;
}

time_t HdfsRdwrMemBuffer::mtime()
{
    // If we've switched to a file buffer
    if (m_pFileBuffer)
    {
        return m_pFileBuffer->mtime();
    }

    // this doesn't really have alot of meaning.  if called here this means
	// the file is open for writing and completely buffered in memory so
	// just return the error value.  Technically could track a modification
	// time in this class if we needed to.
	return -1;
}

int HdfsRdwrMemBuffer::close()
{
	flushImpl();

	delete m_pFileBuffer;
	m_pFileBuffer = NULL;

	m_cur = 0;
	m_last = 0;

	if( IDBLogger::isEnabled() )
		IDBLogger::logNoArg(m_fname, this, "close", 0);
	return 0;
}


}
