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
* $Id: we_cleartablelockcmd.h 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
#ifndef WE_CLEARTABLELOCKCMD_H__
#define WE_CLEARTABLELOCKCMD_H__

#include <string>

#include "bytestream.h"

#include "writeengine.h"

namespace WriteEngine {

/** @brief Process messages from a cleartablelock or filesplitter client
 */
class WE_ClearTableLockCmd
{
public:
	/** @brief WE_ClearTableLockCmd constructor
	 */
	WE_ClearTableLockCmd(const char* userDesc) : fUserDesc(userDesc) { }

	/** @brief Process bulk rollback request
	 *  @param ibs Input byte stream
	 *  @param errMsg Return error message
	 */
	int processRollback(messageqcpp::ByteStream& ibs,
						std::string& errMsg);

	/** @brief Process bulk rollback cleanup request
	 *  @param ibs Input byte stream
	 *  @param errMsg Return error message
	 */
	int processCleanup (messageqcpp::ByteStream& ibs,
						std::string& errMsg);

private:
	WriteEngineWrapper fWEWrapper; // WriteEngineWrapper object
	std::string fUserDesc;
};

}

#endif
