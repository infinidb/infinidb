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

#ifndef BUFFEREDFILE_H_
#define BUFFEREDFILE_H_

#include <stdexcept>
#include <boost/utility.hpp>
#include "IDBDataFile.h"
#include <unistd.h>

#ifdef _MSC_VER
typedef void *HANDLE;
#endif

namespace idbdatafile
{

/**
 * BufferedFile implements the IDBDataFile for I/O to a C library FILE*
 * (via fopen, fwrite, fread, etc.).  See IDBDataFile.h for more documentation
 * on member functions.
 */
class BufferedFile : public IDBDataFile, boost::noncopyable
{
public:
	BufferedFile(const char* fname, const char* mode, unsigned opts);
	/* virtual */ ~BufferedFile();

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
	/* virtual */ int close();

private:
	void	applyOptions( unsigned opts );

	FILE*	m_fp;
	char*   m_buffer;
};

}
#endif /* BUFFEREDFILE_H_ */
