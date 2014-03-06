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

#ifndef IDBPOLICY_H_
#define IDBPOLICY_H_

#include <string>
#include <stdint.h>

#include <boost/thread/mutex.hpp>

#include "IDBDataFile.h"
#include "IDBFileSystem.h"
#include "IDBFactory.h"

namespace idbdatafile
{

/**
 * IDBPolicy is a static class that is used to manage the interaction
 * between the rest of infinidb and the idbdatafile subsystem.  It supports
 * mapping from a file/directory path and context (currently PrimProc or
 * WriteEngineServer) to a file and filesystem type
 */
class IDBPolicy
{
public:
	/**
	 * Define the contexts recognized by IDBPolicy.  The same file may be
	 * opened as a different type depending on the context (for ex. data
	 * files are buffered in WriteEngine and Unbuffered in PrimProc).
	 */
	enum Contexts {
		PRIMPROC,
		WRITEENG
	};

	/**
	 * Config the IDBPolicy based on the Calpont.xml
	 */
	static void configIDBPolicy();


	/**
	 * Initialize the IDBDataFile subsystem.  This should be called once
	 * by the main thread of the application prior to any other use of the
     * library. 
     * bEnableLogging -- for debug only. 
     * bUseRdwrMemBuffer -- If true, use Memory Buffered files (class HdfsRdwrMemBuffer) until hdfsRdwrBufferMaxSize 
     *                    memory is used, then switch to file buffering until mem used is below hdfsRdwrBufferMaxSize.
     * hdfsRdwrScratch -- where to store file-buffered HDFS files (class HdfsRdwrFileBuffer) - it should name
	 *  				  a writable directory with sufficient space to store all needed buffers (size-TBD?).
	 * hdfsRdwrBufferMaxSize -- When RdwrMemBuffers get this big, switch to file buffers .
	 */
	static void init( bool bEnableLogging, bool bUseRdwrMemBuffer, const std::string& hdfsRdwrScratch, int64_t hdfsRdwrBufferMaxSize );

	/**
	 * Load a new filetype plugin.  Return value indicates success(true)
	 * or failure (false)
	 */
	static bool installPlugin(const std::string& plugin);

	/**
	 * Accessor method that returns whether or not HDFS is enabled
	 */
	static bool useHdfs();

	/**
	 * Accessor method that returns whether to use HDFS memory buffers
	 */
	static bool useRdwrMemBuffer();

    /**
     * Accessor method that returns the max amount of mem buff to use before switching to file buffering
     */
    static size_t hdfsRdwrBufferMaxSize();

    /**
     * Accessor method that returns the directory to place our temp hdfs buffer files
     */
    static const std::string& hdfsRdwrScratch();

    /**
	 * getType() returns the proper IDBDataFile::Types for a file given a
	 * path for the path and a context in which it will be interacted with.
	 */
	static IDBDataFile::Types getType( const std::string& path, Contexts ctxt );

	/**
	 * getFs() returns a reference to the proper IDBFileSystem instance that
	 * is able to interact with the file/directory specified by path
	 */
	static IDBFileSystem& getFs( const std::string& path );

	/**
	 * These are convenience functions that simplify the syntax required to
	 * make a filesystem call.  Each of the calls determines the proper
	 * FileSystem to reference for the specified path and then performs the
	 * actual call.  Documentation for these will not be duplicated here -
	 * please see IDBFileSystem.h.
	 */
	static int mkdir(const char *pathname);
	static off64_t size(const char* path);
	static int remove(const char *pathname);
	static int rename(const char *oldpath, const char *newpath);
	static bool exists(const char* pathname);
	static int listDirectory(const char* pathname, std::list<std::string>& contents);
	static bool isDir(const char* pathname);
	static int copyFile(const char* srcPath, const char* destPath);

private:
	/**
	 * don't allow this class to be constructed.  It exposes a purely
	 * static interface
	 */
	IDBPolicy();

	static bool isLocalFile( const std::string& path );

	static bool s_usehdfs;
    static bool s_bUseRdwrMemBuffer;
	static std::string s_hdfsRdwrScratch;
    static int64_t s_hdfsRdwrBufferMaxSize; 
    static bool s_configed;
    static boost::mutex s_mutex;
};

inline
const std::string& IDBPolicy::hdfsRdwrScratch()
{
	return s_hdfsRdwrScratch;
}

inline
bool IDBPolicy::useHdfs()
{
	return s_usehdfs;
}

inline
bool IDBPolicy::useRdwrMemBuffer()
{
	return s_bUseRdwrMemBuffer;
}

inline
size_t IDBPolicy::hdfsRdwrBufferMaxSize()
{
	return s_hdfsRdwrBufferMaxSize;
}

inline
int IDBPolicy::mkdir(const char *pathname)
{
	return IDBPolicy::getFs( pathname ).mkdir( pathname );
}

inline
off64_t IDBPolicy::size(const char* path)
{
	return IDBPolicy::getFs( path ).size( path );
}

inline
int IDBPolicy::remove(const char *pathname)
{
	return IDBPolicy::getFs( pathname ).remove( pathname );
}

inline
int IDBPolicy::rename(const char *oldpath, const char *newpath)
{
	return IDBPolicy::getFs( oldpath ).rename( oldpath, newpath );
}

inline
bool IDBPolicy::exists(const char *pathname)
{
	return IDBPolicy::getFs( pathname ).exists( pathname );
}

inline
int IDBPolicy::listDirectory(const char* pathname, std::list<std::string>& contents)
{
	return IDBPolicy::getFs( pathname ).listDirectory( pathname, contents );
}

inline
bool IDBPolicy::isDir(const char *pathname)
{
	return IDBPolicy::getFs( pathname ).isDir( pathname );
}

inline
int IDBPolicy::copyFile(const char* srcPath, const char* destPath)
{
	return IDBPolicy::getFs( srcPath ).copyFile( srcPath, destPath );
}

}

#endif /* IDBPOLICY_H_ */
