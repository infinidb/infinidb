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

#include "HdfsFileSystem.h"

#include "HdfsFsCache.h"
#include "IDBLogger.h"

#include <iostream>
#include <list>
#include <string.h>

#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>

using namespace std;

namespace idbdatafile
{

HdfsFileSystem::HdfsFileSystem() :
	IDBFileSystem( IDBFileSystem::HDFS )
{
	m_fs = HdfsFsCache::fs();
}

HdfsFileSystem::~HdfsFileSystem()
{
}

int HdfsFileSystem::mkdir(const char *pathname)
{
	// todo: may need to do hdfsChmod to set mode, but for now assume not necessary
	int ret = hdfsCreateDirectory(m_fs,pathname);

	if( ret != 0 )
	{
        std::ostringstream oss;
		oss << "hdfsCreateDirectory failed for: " << pathname << ", errno: " << errno << "," << strerror(errno) << endl;
        IDBLogger::syslog(oss.str(), logging::LOG_TYPE_ERROR);
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logFSop( HDFS, "mkdir", pathname, this, ret);

	return ret;
}

int HdfsFileSystem::remove(const char *pathname)
{
	int ret = 0;

	// the HDFS API doesn't like calling hdfsDelete with a path that does
	// not exist.  We catch this case and return 0 - this is based on
	// boost::filesystem behavior where removing an invalid path is
	// treated as a successful operation
	if( exists( pathname ) )
	{
#ifdef CDH4
		ret = hdfsDelete(m_fs,pathname,1);
#else
		ret = hdfsDelete(m_fs,pathname);
#endif
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logFSop( HDFS, "remove", pathname, this, ret);

	return ret;
}

int HdfsFileSystem::rename(const char *oldpath, const char *newpath)
{
	int ret = hdfsRename(m_fs, oldpath, newpath);

	if( IDBLogger::isEnabled() )
		IDBLogger::logFSop2( HDFS, "rename", oldpath, newpath, this, ret);

	return ret;
}

off64_t HdfsFileSystem::size(const char* path) const
{
	hdfsFileInfo* fileinfo;
	fileinfo = hdfsGetPathInfo(m_fs,path);
	off64_t retval = (fileinfo ? fileinfo->mSize : -1);
	if( fileinfo )
		hdfsFreeFileInfo(fileinfo,1);

	if( IDBLogger::isEnabled() )
		IDBLogger::logFSop( HDFS, "fs:size", path, this, retval);

	return retval;
}

bool HdfsFileSystem::exists(const char *pathname) const
{
	int ret = hdfsExists(m_fs,pathname);
	return ret == 0;
}

int HdfsFileSystem::listDirectory(const char* pathname, std::list<std::string>& contents) const
{
	// clear the return list
	contents.erase( contents.begin(), contents.end() );

	int numEntries;
	hdfsFileInfo* fileinfo;
	if( !exists( pathname ) ) {
		errno = ENOENT;
		return -1;
	}

	// hdfs not happy if you call list directory on a path that does not exist
	fileinfo = hdfsListDirectory(m_fs,pathname, &numEntries);
	for( int i = 0; i < numEntries && fileinfo; ++i )
	{
		// hdfs returns a fully specified path name but we want to
		// only return paths relative to the directory passed in.
	    boost::filesystem::path filepath( fileinfo[i].mName );
		contents.push_back( filepath.filename().c_str() );
	}
	if( fileinfo )
		hdfsFreeFileInfo(fileinfo, numEntries);

	return 0;
}

bool HdfsFileSystem::isDir(const char* pathname) const
{
	hdfsFileInfo* fileinfo;
	fileinfo = hdfsGetPathInfo(m_fs,pathname);
	bool retval = (fileinfo ? fileinfo->mKind == kObjectKindDirectory : false);
	if( fileinfo )
		hdfsFreeFileInfo(fileinfo,1);
	return retval;
}

int HdfsFileSystem::copyFile(const char* srcPath, const char* destPath) const
{
	int ret = hdfsCopy(m_fs, srcPath, m_fs, destPath);

	int n = 0; // retry count
	while (ret != 0 && n++ < 25)
	{
		if (n < 10)
			usleep(10000);
		else
			usleep(200000);

		if( IDBLogger::isEnabled() )
			IDBLogger::logFSop2( HDFS, "copyFile-retry", srcPath, destPath, this, ret);

		ret = hdfsCopy(m_fs, srcPath, m_fs, destPath);
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logFSop2( HDFS, "copyFile", srcPath, destPath, this, ret);

	return ret;
}

bool HdfsFileSystem::filesystemIsUp() const
{
	return (m_fs != NULL);
}

}
