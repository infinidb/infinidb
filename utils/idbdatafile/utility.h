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

#ifndef UTILITY_H_
#define UTILITY_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

namespace idbdatafile
{

/**
 * Take an fopen() style mode string and return the corresponding flags
 * ala open()
 */
inline
int modeStrToFlags(const char* mode)
{
	std::string modestr = mode;
	// this could easily be migrated to a lookup table if performance
	// ever became a concern, but for now this is fine.
    if( modestr == "r" || modestr == "rb" )
	{
		return O_RDONLY;
	}
	else if( modestr == "r+" || modestr == "r+b" )
	{
		return O_RDWR;
	}
	else if( modestr == "w" || modestr == "wb" )
	{
		return O_WRONLY|O_CREAT|O_TRUNC;
	}
	else if( modestr == "w+" || modestr == "w+b" )
	{
		return O_RDWR|O_CREAT|O_TRUNC;
	}
	else if( modestr == "a" || modestr == "ab" )
	{
		return O_WRONLY|O_CREAT|O_APPEND;
	}
	else if( modestr == "a+" || modestr == "a+b" )
	{
		return O_RDWR|O_CREAT|O_APPEND;
	}
	else
	{
		// error
		return -1;
	}
}

#ifdef _MSC_VER
inline
int modeStrToFlags(const char* mode, int& createflags)
{
	std::string modestr = mode;
    createflags = 0;
	// this could easily be migrated to a lookup table if performance
	// ever became a concern, but for now this is fine.
    if( modestr == "r" || modestr == "rb" )
	{
        createflags = OPEN_EXISTING;
        return GENERIC_READ;
	}
	else if( modestr == "r+" || modestr == "r+b" )
	{
        createflags = OPEN_EXISTING;
        return GENERIC_READ | GENERIC_WRITE;
	}
	else if( modestr == "w" || modestr == "wb" )
	{
        createflags = CREATE_ALWAYS;
        return GENERIC_WRITE;
	}
	else if( modestr == "w+" || modestr == "w+b" )
	{
        createflags = CREATE_ALWAYS;
        return GENERIC_READ | GENERIC_WRITE;
	}
	else if( modestr == "a" || modestr == "ab" )
	{
        createflags = OPEN_ALWAYS;
        return FILE_APPEND_DATA;
	}
	else if( modestr == "a+" || modestr == "a+b" )
	{
        createflags = OPEN_ALWAYS;
        return GENERIC_READ | FILE_APPEND_DATA;
	}
	else
	{
		// error
		return -1;
	}
}
#endif

}

#endif /* UTILITY_H_ */
