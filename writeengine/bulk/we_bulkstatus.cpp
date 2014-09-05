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
* $Id: we_bulkstatus.cpp 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/

#include <unistd.h>
#include <cstdlib>

#define WE_BULKSTATUS_DLLEXPORT
#include "we_bulkstatus.h"
#undef WE_BULKSTATUS_DLLEXPORT

namespace WriteEngine
{
    /*static*/
#ifdef _MSC_VER
	volatile LONG BulkStatus::fJobStatus = EXIT_SUCCESS;
#else
	volatile int BulkStatus::fJobStatus = EXIT_SUCCESS;
#endif
//------------------------------------------------------------------------------
// Set the job status
//------------------------------------------------------------------------------
/* static */
void BulkStatus::setJobStatus(int jobStatus)
{
#ifdef _MSC_VER
    (void)InterlockedCompareExchange (&fJobStatus, jobStatus, EXIT_SUCCESS);
#else
    (void)__sync_val_compare_and_swap(&fJobStatus, EXIT_SUCCESS, jobStatus);
 
#endif
}

}
