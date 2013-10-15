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

#ifndef HDFSFSCACHE_H_
#define HDFSFSCACHE_H_

#include <stdexcept>

#include "hdfs.h"

namespace idbdatafile
{

/**
 * The HdfsFsCache class is a simple class that manages the connection
 * to the HDFS file system.  Per the libhdfs documentation the hdfsConnect
 * function returns the same handle when called from the same process and
 * this is a slow call so we only want to make one.  Further, the use of the
 * return handle is completely threadsafe.  The fs() method is thread-safe
 * via mutex - it is expected that the main users of the handle will cache
 * for their lifetime and thus the mutex performance hit is minimal.
 */
class HdfsFsCache
{
public:
	/**
	 * accessor method to retrieve the hdfsFS handle
	 */
	static hdfsFS fs();

private:
	static hdfsFS s_fs;
};

}

#endif /* HDFSFSCACHE_H_ */
