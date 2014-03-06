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

#include "HdfsRdwrFileBuffer.h"
#include "HdfsRdwrMemBuffer.h"
#include <stdlib.h>
#include <iostream>
#include <string>
#include <sstream>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>
#include <boost/scoped_array.hpp>

#include "IDBPolicy.h"
#include "BufferedFile.h"
#include "HdfsFile.h"
#include "IDBLogger.h"
using namespace std;

namespace idbdatafile
{

const size_t BUFSIZE = 1 * 1024 * 1024; // 1MB

HdfsRdwrFileBuffer::HdfsRdwrFileBuffer(const char* fname, const char* mode, unsigned opts) :
	IDBDataFile(fname),
	m_buffer(NULL),
	m_dirty(false),
	m_new(false)
{
	// we have been asked to support rw operations on a file that currently
	// exists in HDFS by using a local file buffer.

	IDBFileSystem& fs = IDBFactory::getFs( HDFS );

	// append an extra tmp extension if USE_TMPFILE extension is set.
	string input(fname);
	if (opts & USE_TMPFILE)
	{
		m_fname += ".tmp";   // use .tmp as working copy to replace the ".hdr/chk" backup.
		if (IDBPolicy::exists(m_fname.c_str()))
			input = m_fname; // get from .tmp if revisit
		else
			m_new = true;
	}

	// cache the size first - we will use this to determine if the
	// file already exists and needs to be read into the buffer
	off64_t size = fs.size(input.c_str());

	// first we will try to open the file for writing - if this fails
	// it means we can't actually open the file for writing and need
	// to throw exception anyway
	bool truncate = (mode != NULL && *mode == 'w');
	if( size < 0 || truncate)
	{
		HdfsFile* trywrite = new HdfsFile(input.c_str(), "w", 0);
		delete trywrite;

		size = 0;
	}

	// if we get here, then will be ok to write later.  First we
	// we set up the local directory that we need to write to
	string bufname = IDBPolicy::hdfsRdwrScratch() + input;
	boost::filesystem::path pathDir(bufname);

	// this will create the directory we want to work in
	if( IDBPolicy::mkdir(pathDir.parent_path().string().c_str()) != 0 )
	{
		ostringstream oss;
		oss << "Unable to create directory path: " << pathDir.parent_path();
		throw std::runtime_error(oss.str());
	}

	m_buffer = new BufferedFile( bufname.c_str(), "w+", IDBDataFile::USE_VBUF );

	// Now we need to
	// open one for reading if the file existed previously
	if( size > 0 )
	{
		HdfsFile tryread(input.c_str(), "r", 0);

		boost::scoped_array<unsigned char> buffer(new unsigned char[BUFSIZE]);
		ssize_t bytesProcessed = 0;
		while(bytesProcessed < (ssize_t) size)
		{
			ssize_t bytesRead = tryread.read( buffer.get(), BUFSIZE );
			m_buffer->write( buffer.get(), bytesRead );
			bytesProcessed += bytesRead;
		}
		m_buffer->seek(0,SEEK_SET);
	}
	else
	{
		// in this case there is no existing file to read and nothing else to do
		;
	}
}

// This constructor is for use by HdfsRdwrMemBuffer to create a file buffer when we
// run out of memory.
HdfsRdwrFileBuffer::HdfsRdwrFileBuffer(HdfsRdwrMemBuffer* pMemBuffer) throw (std::exception) :
	IDBDataFile(pMemBuffer->name().c_str()),
	m_buffer(NULL),
	m_dirty(false)
{
	// we have been asked to replace memory buffered rw operations with file buffered 
    // operations on a file that currently exists in HDFS.

	// Set up the local directory that we need to write to
	string bufname = IDBPolicy::hdfsRdwrScratch() + name();
	boost::filesystem::path pathDir(bufname);

    // this will create the directory we want to work in
	if( IDBPolicy::mkdir(pathDir.parent_path().string().c_str()) != 0 )
	{
		ostringstream oss;
		oss << "MemBuffer overflow. Unable to create directory path: " << pathDir.parent_path();
		throw std::runtime_error(oss.str());
	}

	m_buffer = new BufferedFile( bufname.c_str(), "w+", IDBDataFile::USE_VBUF );

	// Dump the contents of the memory buffer into the file
    const unsigned char* membuffer = pMemBuffer->getbuffer();
    ssize_t bytesToProcess = pMemBuffer->size();
    ssize_t bytesProcessed = 0;
    while (bytesToProcess > 0)
    {
        bytesProcessed = m_buffer->write( membuffer, bytesToProcess );
        if (bytesProcessed < 0 && errno != EINTR)
        {
            ostringstream oss;
            oss << "MemBuffer overflow. Error while writing: " << pathDir << " " << strerror(errno);
            throw std::runtime_error(oss.str());
        }
        membuffer += bytesProcessed;
        bytesToProcess -= bytesProcessed;
    }
}

HdfsRdwrFileBuffer::~HdfsRdwrFileBuffer()
{
	close();
}

ssize_t HdfsRdwrFileBuffer::pread(void *ptr, off64_t offset, size_t count)
{
	return m_buffer->pread(ptr, offset, count);
}

ssize_t HdfsRdwrFileBuffer::read(void *ptr, size_t count)
{
	return m_buffer->read(ptr, count);
}

ssize_t HdfsRdwrFileBuffer::write(const void *ptr, size_t count)
{
	m_dirty = true;
    return m_buffer->write(ptr, count);
}

int HdfsRdwrFileBuffer::seek(off64_t offset, int whence)
{
	return m_buffer->seek(offset, whence);
}

int HdfsRdwrFileBuffer::truncate(off64_t length)
{
	m_dirty = true;
	return m_buffer->truncate(length);
}

off64_t HdfsRdwrFileBuffer::size()
{
	return m_buffer->size();
}

off64_t HdfsRdwrFileBuffer::tell()
{
	return m_buffer->tell();
}

int HdfsRdwrFileBuffer::flush()
{
	int ret = 0;

	if (m_dirty || m_new)
	{
		// no need to flush the tmp file:
		//     this the owner and the file is not for others to read.
		//     BufferedFile::size() uses ftell get the file current size.
		// ret =  m_buffer->flush();

		size_t size = m_buffer->size();

		HdfsFile writer(m_fname.c_str(), "w", 0);

		boost::scoped_array<unsigned char> buffer(new unsigned char[BUFSIZE]);

		ssize_t bytesProcessed = 0;
		assert( m_buffer->seek(0, SEEK_SET) == 0 );
		assert( m_buffer->tell() == 0 );
		while(bytesProcessed < (ssize_t) size)
		{
			ssize_t bytesToRead = min( BUFSIZE, size - bytesProcessed );
			ssize_t bytesRead = m_buffer->read( buffer.get(), bytesToRead );
			assert( bytesRead > 0 );
			writer.write( buffer.get(), bytesRead );
			bytesProcessed += bytesRead;
		}

		m_dirty = false;
		m_new = false;
	}

	return ret;
}

time_t HdfsRdwrFileBuffer::mtime()
{
	return m_buffer->mtime();
}

void HdfsRdwrFileBuffer::close()
{
	// on close, flush data from tmp file back to hdfs
	flush();

	string buffile = m_buffer->name();
	// now cleanup our buffer
	if( IDBPolicy::remove(buffile.c_str()) != 0 )
	{
		ostringstream oss;
		oss << "Unable to remove HdfsRdwr buffer file: " << buffile;
		throw std::runtime_error(oss.str());
	}

	// delete will close the BufferedFile
	delete m_buffer;
	m_buffer = 0;
}

}
