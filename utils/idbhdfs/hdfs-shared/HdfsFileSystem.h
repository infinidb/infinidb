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

#ifndef HDFSFILESYSTEM_H_
#define HDFSFILESYSTEM_H_

#include <boost/utility.hpp>
#include "hdfs.h"
#include "IDBFileSystem.h"

namespace idbdatafile
{

class HdfsFileSystem : public IDBFileSystem, boost::noncopyable
{
public:
	HdfsFileSystem();
	/* virtual */ ~HdfsFileSystem();

	/* virtual */ int mkdir(const char *pathname);
	/* virtual */ off64_t size(const char* path) const;
	/* virtual */ int remove(const char *pathname);
	/* virtual */ int rename(const char *oldpath, const char *newpath);
	/* virtual */ bool exists(const char* pathname) const;
	/* virtual */ int listDirectory(const char* pathname, std::list<std::string>& contents) const;
	/* virtual */ bool isDir(const char* pathname) const;
	/* virtual */ int copyFile(const char* srcPath, const char* destPath) const;
	/* virtual */ bool filesystemIsUp() const;

private:
	hdfsFS   m_fs;
};

}

#endif /* HDFSFILESYSTEM_H_ */
