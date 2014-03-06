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
* $Id: loggingid.h 3495 2013-01-21 14:09:51Z rdempsey $
*
******************************************************************************************/
/**
 * @file
 */
#ifndef LOGGING_LOGGINGID_H
#define LOGGING_LOGGINGID_H

#include <string>

namespace logging {

/** @brief a logging context structure
 *
 */
struct LoggingID
{
	/** @brief LoggingID ctor
	 *
	 */
	explicit LoggingID(unsigned subsysID=0, unsigned sessionID=0, unsigned txnID=0, unsigned ThdID=0)
		: fSubsysID(subsysID), fSessionID(sessionID), fTxnID(txnID), fThdID(ThdID)
		{}

	unsigned fSubsysID;	/// subsystem ID
	unsigned fSessionID;	/// session ID
	unsigned fTxnID;	/// transaction ID
	unsigned fThdID;	/// thread ID
};

}

#endif
