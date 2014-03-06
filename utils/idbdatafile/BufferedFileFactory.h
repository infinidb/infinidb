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

#ifndef BUFFEREDFILEFACTORY_H_
#define BUFFEREDFILEFACTORY_H_

#include "FileFactoryBase.h"
#include "BufferedFile.h"
#include "PosixFileSystem.h"

namespace idbdatafile
{

class BufferedFileFactory : public FileFactoryBase
{
public:
	/* virtual */ IDBDataFile* open(const char* fname, const char* mode, unsigned opts, unsigned colWidth);
};

inline
IDBDataFile* BufferedFileFactory::open(const char* fname, const char* mode, unsigned opts, unsigned /*defsize*/)
{
	return new BufferedFile(fname, mode, opts);
}

}
#endif /* BUFFEREDFILEFACTORY_H_ */
