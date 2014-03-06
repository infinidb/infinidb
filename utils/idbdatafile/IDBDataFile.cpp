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

#include "IDBDataFile.h"

#include <stdexcept>
#include <sys/types.h>
#include <sstream>
#include <iostream>

#include "UnbufferedFile.h"
#include "BufferedFile.h"
#include "IDBLogger.h"
#include "IDBFactory.h"

using namespace std;

namespace idbdatafile
{

IDBDataFile* IDBDataFile::open(Types type, const char* fname, const char* mode, unsigned opts, unsigned colWidth)
{
	IDBDataFile* ret = 0;

	try
	{
		ret = IDBFactory::open(type, fname, mode, opts, colWidth);
	}
	catch (std::exception& e)
	{
    	std::ostringstream oss;
		oss << "Failed to open file: " << fname << ", exception: " << e.what();
    	IDBLogger::syslog(oss.str(), logging::LOG_TYPE_ERROR);
		if( IDBLogger::isEnabled() )
			IDBLogger::logNoArg( fname, 0, e.what(), 0 );
	}

	if( IDBLogger::isEnabled() )
		IDBLogger::logOpen( type, fname, mode, opts, ret );
	return ret;
}

}
