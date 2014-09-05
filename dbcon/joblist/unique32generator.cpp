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

/******************************************************************************
 * $Id: unique32generator.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *****************************************************************************/

/** @file Contains class that controls unique 32-bit generation for joblist.
 *
 */

#include "unique32generator.h"

#include <stdint.h>
#include <boost/thread.hpp>

#include "dbrm.h"

namespace joblist {

/* static */ UniqueNumberGenerator* UniqueNumberGenerator::fUnique32Generator = 0;
/* static */ boost::mutex       UniqueNumberGenerator::fLock;

//------------------------------------------------------------------------------
// Accessor to singleton handle
//------------------------------------------------------------------------------
/* static */
UniqueNumberGenerator* UniqueNumberGenerator::instance()
{
	boost::mutex::scoped_lock lk(fLock);

	if ( !fUnique32Generator )
	{
		fUnique32Generator = new UniqueNumberGenerator();
	}

	return fUnique32Generator;
}

//------------------------------------------------------------------------------
// Deletes singleton instance (not necessary for application to call this,
// but we make it available in case we ever want to clean up correctly; else
// valgrind may report a memory leak for not deleting our heap memory).
//------------------------------------------------------------------------------
/* static */
void UniqueNumberGenerator::deleteInstance()
{
	boost::mutex::scoped_lock lk(fLock);

	if ( fUnique32Generator )
	{
		delete fUnique32Generator;
		fUnique32Generator = 0;
	}
}

//------------------------------------------------------------------------------
// Return a system-wide unique32 bit integer
//------------------------------------------------------------------------------
uint32_t UniqueNumberGenerator::getUnique32()
{
	return fDbrm.getUnique32();
}

uint64_t UniqueNumberGenerator::getUnique64()
{
	return fDbrm.getUnique64();
}

} // namespace
