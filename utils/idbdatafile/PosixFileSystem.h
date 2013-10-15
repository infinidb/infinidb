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

#ifndef POSIXFILESYSTEM_H_
#define POSIXFILESYSTEM_H_

#include "IDBFileSystem.h"

namespace idbdatafile
{

class PosixFileSystem : public IDBFileSystem
{
public:
	PosixFileSystem();
	/* virtual */  ~PosixFileSystem();

	/* virtual */ int mkdir(const char *pathname);
	/* virtual */ off64_t size(const char* path) const;
	/* virtual */ int remove(const char *pathname);
	/* virtual */ int rename(const char *oldpath, const char *newpath);
	/* virtual */ bool exists(const char* pathname) const;
	/* virtual */ int listDirectory(const char* pathname, std::list<std::string>& contents) const;
	/* virtual */ bool isDir(const char* pathname) const;
	/* virtual */ int copyFile(const char* srcPath, const char* destPath) const;
};

}

#endif /* POSIXFILESYSTEM_H_ */
