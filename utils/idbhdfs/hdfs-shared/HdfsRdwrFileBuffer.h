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

#ifndef HDFSRDWRFILEBUFFER_H_
#define HDFSRDWRFILEBUFFER_H_

#include <stdexcept>
#include <boost/utility.hpp>
#include "hdfs.h"

#include "IDBDataFile.h"

namespace idbdatafile
{
class BufferedFile;
class HdfsRdwrMemBuffer;
/**
 * HdfsRdwrFileBuffer implements IDBDataFile for a file that exists in HDFS
 * but that the application needs rdwr access to and wants to use a standard
 * POSIX file buffer.  All operations occur against a file in the local file
 * system and are copied over to HDFS when the file is closed
 */
class HdfsRdwrFileBuffer: public IDBDataFile, boost::noncopyable
{
public:
	HdfsRdwrFileBuffer(const char* fname, const char* mode, unsigned opts);
	HdfsRdwrFileBuffer(HdfsRdwrMemBuffer* pMemBuffer) throw (std::exception);
	/* virtual */ ~HdfsRdwrFileBuffer();

	/* virtual */ ssize_t pread(void *ptr, off64_t offset, size_t count);
	/* virtual */ ssize_t read(void *ptr, size_t count);
	/* virtual */ ssize_t write(const void *ptr, size_t count);
	/* virtual */ int seek(off64_t offset, int whence);
	/* virtual */ int truncate(off64_t length);
	/* virtual */ off64_t size();
	/* virtual */ off64_t tell();
	/* virtual */ int flush();
	/* virtual */ time_t mtime();

protected:
	/* virtual */ void close();

private:
	BufferedFile* m_buffer;
	bool          m_dirty;
	bool          m_new;
};

}

#endif /* HDFSRDWRFILEBUFFER_H_ */
