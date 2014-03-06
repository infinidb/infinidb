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

/*
 * rwlockmonitor.cpp
 *
 *  Created on: Aug 15, 2011
 *      Author: pleblanc
 */
#include <unistd.h>
#include <iostream>
#define RWLOCKMONITOR_DLLEXPORT
#include "rwlockmonitor.h"
#undef RWLOCKMONITOR_DLLEXPORT
#include "logger.h"
#include "errorids.h"

using namespace std;
using namespace rwlock;
using namespace logging;

namespace BRM {

RWLockMonitor::RWLockMonitor(const bool *d, const bool *ls, const uint32_t k) :
		die(d), lockStatus(ls), key(k)
{
	ts.tv_sec = 210;   // 3:30 timer
	ts.tv_nsec = 0;
	secsBetweenAttempts = 30;
	lock.reset(new RWLock(key));
}

RWLockMonitor::~RWLockMonitor()
{
}

void RWLockMonitor::operator()()
{

/*
 * Grab a timed write lock.
 *   on failure
 *     if there's an active reader, do read_unlock()
 *     log everything else.
 *     *** write lock fixing is being postponed for now.
 */

	LockState state;
	bool gotTheLock;
	bool reportedProblem = false;
	Logger logger(30);

	while (!(*die)) {
		gotTheLock = lock->timed_write_lock(ts, &state);
		if (*die)
			break;
		if (!gotTheLock) {
			if (state.mutexLocked) {
				if (!reportedProblem) {
					//Message msg(ERR_BRM_MUTEX);
					Message msg(M0092);
					logger.logMessage(LOG_TYPE_CRITICAL, msg, LoggingID());
					reportedProblem = true;
				}
			}

			else if (state.reading > 0) {
				if (!reportedProblem) {
					//Message msg(ERR_RECOVERABLE_LOCK_STATE);
					Message msg(M0094);
					Message::Args args;

					args.add(state.reading);
					args.add(state.readerswaiting);
					args.add(state.writing);
					args.add(state.writerswaiting);
					msg.format(args);
					logger.logMessage(LOG_TYPE_WARNING, msg, LoggingID());
					reportedProblem = true;
				}
				for (int i = 0; i < state.reading; i++)
					lock->read_unlock();
			}

			// the write lock is held but not by this process, not good.
			// there's a slight race here between these two vars but it's miniscule,
			// and the worst thing that happens is a false positive error msg.
			else if (state.writing > 0 && !(*lockStatus)) {
				if (!reportedProblem) {
					//Message msg(ERR_UNRECOVERABLE_LOCK_STATE);
					Message msg(M0093);
					Message::Args args;

					args.add(state.reading);
					args.add(state.readerswaiting);
					args.add(state.writing);
					args.add(state.writerswaiting);
					msg.format(args);
					logger.logMessage(LOG_TYPE_CRITICAL, msg, LoggingID());
					reportedProblem = true;
				}

				/* put write lock recovery code here */
			}
			else {
				// the workernode is legitmately taking a long time
				//cout << "holds the lock. " << " r=" << state.reading << " rwt=" << state.readerswaiting <<
				//		" w=" << state.writing << " wwt=" << state.writerswaiting << endl;
			}
		}
		else {
			/* got the write lock.  If there was a problem before it's been fixed. */
			lock->write_unlock();
			if (reportedProblem) {
				//Message msg(ERR_SUCCESSFUL_RECOVERY);
				Message msg(M0095);
				logger.logMessage(LOG_TYPE_WARNING, msg, LoggingID());
				reportedProblem = false;
			}
			sleep(secsBetweenAttempts);
		}
	}
}

} /* namespace BRM */
