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
 * $Id: monitorprocmem.h 1588 2011-02-08 14:37:03Z rdempsey $
 *
 *****************************************************************************/

/** @file StagedDL datalist used to process data concurrently in stages.
 *
 */

#ifndef MONITORPROCMEM_H__
#define MONITORPROCMEM_H__

#include <sys/types.h>
#include <unistd.h>

#include "pp_logger.h"

namespace primitiveprocessor
{

/** @brief A class to monitor a process's memory usage.
 *
 * This class will monitor and terminate the process if
 * this process exceeds the specified memory limit.
 */
class MonitorProcMem
{
    public:
        /** @brief MonitorProcMem constructor
         *
         * @param maxPct (in) maximum allowable memory usage
         * @param msgLog (in) message logger to log msg to
         * @param sec    (in) number of seconds between memory checks
         */
        explicit MonitorProcMem(size_t   maxPct,
                                Logger*  msgLog,
                                unsigned sec=15) :
                 fPid     ( getpid() ),
                 fMaxPct  ( maxPct   ),
                 fSleepSec( sec ),
                 fMsgLog  ( msgLog ),
                 fPageSize( getpagesize() ) { }

        /** @brief Thread entry point
         *
         * Entry point for this thread that monitors memory usage.
         */
        void operator()() const;

    private:
        //Defaults are okay
        //MonitorProcMem  (const MonitorProcMem& rhs);
        //MonitorProcMem& operator=(const MonitorProcMem& rhs);

        /** return the current process RSS size in MB
         *
         */
        size_t rss() const;

        /* return the system RAM size in MB
         *
         */
        size_t memTotal() const;

        /* pause for fSleepSec seconds between memory usage checks
         *
         */
        void pause_() const;

        pid_t    fPid;       // process pid
        size_t   fMaxPct;    // max allowable % memory use
        unsigned fSleepSec;  // sleep interval in seconds
        Logger*  fMsgLog;    // Logger used to record error msg
        int      fPageSize;  // page size for this host (in bytes)
};

}

#endif
