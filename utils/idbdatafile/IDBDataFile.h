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

#ifndef IDBDATAFILE_H_
#define IDBDATAFILE_H_
#define _FILE_OFFSET_BITS 64
#include <stdlib.h>
#include <string>
#include "largefile64.h"
#include<sys/types.h>
#include "stdint.h"
#include <unistd.h>
#ifdef _MSC_VER
#undef tell
#else
#define INVALID_HANDLE_VALUE 0      // Defined already in MSC
#endif

namespace idbdatafile
{

/**
 * IDBDataFile is a combination abstract base class and factory.  The purpose
 * is to encapsulate different underlying storage types from the InfiniDB
 * engine.  The interface is designed so that behavior is consistent across
 * storage types, knowing that options like HDFS are somewhat limited as
 * compared to normal POSIX file systems.
 *
 * to-do think about const-correctness
 */
class IDBDataFile
{
public:
	/**
	 * The TYPE enum defines the supported underlying storage types
	 */
	enum Types {
		UNKNOWN    = 0x0000,
		BUFFERED   = 0x0001,
		UNBUFFERED = 0x0002,
		HDFS	   = 0x0003,
	};

	/**
	 * The Options enum defines elements of a bitmask that can be passed into
	 * the open method for controlling specific options relating to the
	 * particular file type in question.  If an option is unsupported by the
	 * particular type the open() method will fail.
	 *   USE_ODIRECT - requests the O_DIRECT flag to be set, only for
	 *                 unbuffered files
	 *   USE_VBUF    - requests that a non-default buffer is use for a
	 *                 buffered file.  For now, the size of the buffer is
	 *                 hard-coded to match DEFAULT_BUFSIZ in writeengine
	 *                 (1*1024*1024)
	 *   USE_NOVBUF  - requests that buffering is disabled when using a
	 *                 buffered type file
	 *   USE_TMPFILE - requests to output the membuffer to a temp file i/o
	 *                 overwrite the original file
	 */
	enum Options {
		USE_ODIRECT = 0x0001,
		USE_VBUF    = 0x0002,
		USE_NOVBUF  = 0x0004,
		USE_TMPFILE = 0x0008
	};

	/**
	 * DEFSIZE is used as the starting size for the memory buffer of read/write
	 * HDFS files.  Thes files are entirely buffered in memory and modified
	 * there since HDFS does not support any file updates.  This value was
     * engineered to accomodate the standard default extent for a 4-byte column 
     * <Deprecated> 
	 */
	static const int DEFSIZE = 33562624;
    /**
     * EXTENTSIZE is used with the passed in col width the starting size for the 
     * memory buffer of read/write HDFS files.  Thes files are entirely buffered 
     * in memory and modified there since HDFS does not support any file updates. 
     */
    static const int EXTENTSIZE = 8390656;

	/**
	 * This is an alternate factory method that accepts a typical mode
	 * string (ala fopen).  Note that in general IDBDataFile only recognizes
	 * a subset of the possible mode string that fopen supports however
	 * at present time, the mode string will be passed through directly to
	 * the BufferedFile constructor and through to fopen.  The eventual goal
	 * is not complete support but rather only that necessary for Infinidb so
	 * this will be reconsidered.
	 */
	static IDBDataFile* open(Types type, const char* fname, const char* mode, unsigned opts, unsigned colWidth = 4);

	/**
	 * Library users should assume that the destructor closes the file.
	 * This actually occurs in the derived clasess - hence the protected
	 * member close() here
	 */
	virtual ~IDBDataFile();

	/**
	 * This returns the name of the file.
	 */
	const std::string& name() const;

	/**
	 * This is a positional read method similar to kernel style pread
	 * or fseek followed by read for C-library FILE*.  Return value
	 * is the number of bytes read.
	 */
	virtual ssize_t pread(void *ptr, off64_t offset, size_t count) = 0;

	/**
	 * This is a read method similar to kernel style read or C library
	 * fread().  Return value is the number of bytes read.
	 */
	virtual ssize_t read(void *ptr, size_t count) = 0;

	/**
	 * The write() call semantics match the standard library.  There is
	 * no positional write and further there is no lseek or equivalent so
	 * all writing must therefore occur sequentially - either from the
	 * beginning of a file (open via WRITE) or the end of a file (open via
	 * APPEND).  Return value is the number of bytes written.
	 */
	virtual ssize_t write(const void *ptr, size_t count) = 0;

	/**
	 * The seek() method is equivalent to the lseek() and fseek() functions.
	 * The whence parameter accepts SEEK_SET, SEEK_CUR, SEEK_END just as
	 * those functions do.  Note that not all file systems support this
	 * operation - ex. HDFS will not support it for files opened for writing
	 * Returns 0 on success, -1 on error
	 */
	virtual int seek(off64_t offset, int whence) = 0;

	/**
	 * The truncate() method is equivalent to the ftruncate method.  Note
	 * that not all file types support this operation - ex.  HDFS files opened
	 * or write or append do not, but HDFS files opened for modification do.
	 * Returns 0 on success, -1 on error.
	 */
	virtual int truncate(off64_t length) = 0;

	/**
	 * The size() method returns the size of the file in a manner consistent
	 * with the underlying filesystem.  Note that this method will always
	 * return the correct size from the perspective of the open file handle,
	 * thus depending on the semantics of the underlying file system an
	 * external view of size may differ (ex. if writing buffered i/o before
	 * a flush/sync or if writing an open HDFS file).  Returns -1 on error.
	 */
	virtual off64_t size() = 0;

	/**
	 * The tell() call returns the current offset in the file.  This is
	 * similar to lseek with 0 offset in the standard library and ftell
	 * for buffered FILE *s.
	 */
	virtual off64_t tell() = 0;

	/**
	 * The flush() method instructs the file to write any buffered contents
	 * to disk.  Where relevant this method will include a call to fsync
	 */
	virtual int flush() = 0;

	/**
	 * The mtime() method returns the modification time of the file in
	 * seconds.  Returns -1 on error.
	 */
	virtual time_t mtime() = 0;

    int colWidth() {return m_fColWidth;}

protected:
	/**
	 * Constructor - takes the filename to be stored in a member variable
	 * for logging purposes
	 */
	IDBDataFile( const char* fname );

	/**
	 * The close() method closes the file.  It is defined as protected
	 * because the preference is for close() to happen automatically during
	 * deletion of the object
	 */
	virtual int close() = 0;

	/**
	 * file name
	 */
	std::string m_fname;

    /**
     * Column width. If not applicable, defaults to 4. 
     * We use this to determine the initial size of hdfs ram buffers. 
     */
    int m_fColWidth;

};

inline
IDBDataFile::IDBDataFile( const char* fname ) :
	m_fname( fname ),
    m_fColWidth( 4 )
{
}

inline
IDBDataFile::~IDBDataFile()
{
}

inline
const std::string& IDBDataFile::name() const
{
	return m_fname;
}

}
#endif /* IDBDATAFILE_H_ */
