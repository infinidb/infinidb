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

#ifndef HDFSFILE_H_
#define HDFSFILE_H_

#include <stdexcept>
#include <boost/utility.hpp>
#include <boost/thread.hpp>

#include "hdfs.h"

#include "IDBDataFile.h"

namespace idbdatafile
{

/**
 * the HdfsFile implements IDBDataFile for a file in the Hadoop file system
 * (HDFS).  HDFS implements only a subset of POSIX file capability but the
 * IDBDataFile interface was limited to what HDFS can support.  See
 * IDBDataFile documentation for further description of member functions
 */
class HdfsFile: public IDBDataFile, boost::noncopyable
{
public:
	HdfsFile(const char* fname, const char* mode, unsigned opts);
	/* virtual */ ~HdfsFile();

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
	void reopen() throw (std::exception);

	volatile hdfsFile m_file;
	hdfsFS   m_fs;
	int      m_flags;
	boost::mutex m_mutex;
};

}

#endif /* HDFSFILE_H_ */
