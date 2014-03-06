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

// $Id: activestatementcounter.h 940 2013-01-21 14:11:31Z rdempsey $
//
/** @file */

#ifndef ACTIVESTATEMENTCOUNTER_H__
#define ACTIVESTATEMENTCOUNTER_H__

#include <stdint.h>

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

#include "vss.h"

class ActiveStatementCounter
{
public:
	ActiveStatementCounter(uint32_t limit) :
		fStatementCount(0),
		upperLimit(limit),
		fStatementsWaiting(0)
		{}

	virtual ~ActiveStatementCounter() {}

	void incr(bool& counted);
	void decr(bool& counted);
	uint32_t cur() const { return fStatementCount; }
	uint32_t waiting() const { return fStatementsWaiting; }

private:
	ActiveStatementCounter(const ActiveStatementCounter& rhs);
	ActiveStatementCounter& operator=(const ActiveStatementCounter& rhs);

	uint32_t fStatementCount;
	uint32_t upperLimit;
	uint32_t fStatementsWaiting;
	boost::mutex fMutex;
	boost::condition condvar;
	BRM::VSS fVss;
};

#endif
// vim:ts=4 sw=4:

