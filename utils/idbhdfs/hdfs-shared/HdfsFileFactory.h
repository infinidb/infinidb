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

#ifndef HDFSFILEFACTORY_H_
#define HDFSFILEFACTORY_H_

#include <string>

#include "FileFactoryBase.h"
#include "HdfsFile.h"
#include "HdfsRdwrFileBuffer.h"
#include "HdfsRdwrMemBuffer.h"
#include "IDBPolicy.h"
#include "IDBLogger.h"
#include "MonitorProcMem.h"

namespace idbdatafile
{

class HdfsFileFactory : public FileFactoryBase
{
public:
	/* virtual */ IDBDataFile* open(const char* fname, const char* mode, unsigned opts, unsigned colWidth);
};

inline
IDBDataFile* HdfsFileFactory::open(const char* fname, const char* mode, unsigned opts, unsigned colWidth )
{
	std::string modestr = mode;
	bool rdwr = modestr.find('+') != std::string::npos;

	if( rdwr )
	{
		// If the useRdwrMemBuffer switch is turned on (default = on)
		// and we haven't exceeded our max, if any
        // and there's memory to be had,
		// use the membuffer.
        size_t bufSize = IDBDataFile::EXTENTSIZE;
		if( IDBPolicy::useRdwrMemBuffer() &&
            (IDBPolicy::hdfsRdwrBufferMaxSize() == 0 || (HdfsRdwrMemBuffer::getTotalBuff() + bufSize) < IDBPolicy::hdfsRdwrBufferMaxSize()) &&
            utils::MonitorProcMem::isMemAvailable(bufSize))
        {
			return new HdfsRdwrMemBuffer( fname, mode, opts, colWidth );
        }
		else
        {
			return new HdfsRdwrFileBuffer( fname, mode, opts );
        }
	}
	else
	{
		return new HdfsFile( fname, mode, opts );
	}
}

}
#endif /* HDFSFILEFACTORY_H_ */
