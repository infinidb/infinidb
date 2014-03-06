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

/******************************************************************************************
* $Id$
*
******************************************************************************************/
/**
 * @file
 */
#ifndef STARTUP_INSTALLDIR_H__
#define STARTUP_INSTALLDIR_H__

#include <string>
#include <boost/thread/mutex.hpp>

namespace startup
{

/** class */
class StartUp
{
public:
	StartUp() {}
	~StartUp() {}

	static const std::string installDir();

private:
	StartUp(const StartUp& rhs);
	StartUp& operator=(const StartUp& rhs);

	static boost::mutex fInstallDirLock;
	static std::string* fInstallDirp;
};

}

#endif
// vim:ts=4 sw=4:

