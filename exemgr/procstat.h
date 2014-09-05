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

// $Id: procstat.h 940 2013-01-21 14:11:31Z rdempsey $

/** @file */

#ifndef PROCSTAT_H__
#define PROCSTAT_H__

#include <sys/types.h>
#include <unistd.h>

namespace procstat
{

/** A class to monitor a process's status
 *
 */
class ProcStat
{
public:
	/** ctor
	 *
	 */
	explicit ProcStat(pid_t pid) : fPid(pid), fPagesize(getpagesize()) { }

	/** return the current process RSS size in MB
	 *
	 */
	size_t rss() const;

	/** return the system RAM size in MB
	 *
	 */
	static size_t memTotal();

private:
	//defaults okay
	//ProcStat(const ProcStat& rhs);
	//ProcStat& operator=(const ProcStat& rhs);

	pid_t fPid;
	int fPagesize;
};

}

#endif

