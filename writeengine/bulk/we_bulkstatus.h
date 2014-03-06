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

/*******************************************************************************
* $Id: we_bulkstatus.h 4648 2013-05-29 21:42:40Z rdempsey $
*
*******************************************************************************/
/** @file */

#ifndef _WE_BULKSTATUS_H_
#define _WE_BULKSTATUS_H_

#if 0 //defined(_MSC_VER) && defined(WE_BULKSTATUS_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace WriteEngine
{

// Defined this class to hold the global JobStatus flag rather then storing in
// BulkLoad, because that would introduce circular dependencies, with the other
// classes needing BulkLoad.  So put the JobStatus in a separate class.
class BulkStatus
{
public:
   static int     getJobStatus()           { return fJobStatus; }
   static void setJobStatus(int jobStatus) { fJobStatus = jobStatus; }

private:
    /* @brief Global job status flag.
    * Declared volatile to insure that all threads see when this flag is
    * changed.  We don't worry about using a mutex since we are just using
    * as a flag.  Making the variable volatile should suffice, to make it
    * work with multiple threads.
    */
   static volatile int fJobStatus;
};

} // end of namespace

#undef EXPORT

#endif // _WE_BULKSTATUS_H_
