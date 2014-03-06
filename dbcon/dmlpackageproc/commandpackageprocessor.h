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

/***********************************************************************
*   $Id: commandpackageprocessor.h 9302 2013-03-07 16:06:59Z chao $
*
*
***********************************************************************/
/** @file */

#ifndef COMMANDPACKAGEPROCESSOR_H
#define COMMANDPACKAGEPROCESSOR_H
#include <string>
#include <vector>
#include <set>
#include <boost/any.hpp>
#include <boost/algorithm/string.hpp>
#include "dmlpackageprocessor.h"
#include "dmltable.h"
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

#if defined(_MSC_VER) && defined(DMLPKGPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace dmlpackageprocessor
{
/** @brief concrete implementation of a DMLPackageProcessor.
  * Specifically for interacting with the Write Engine to
  * process INSERT dml statements.
  */
class CommandPackageProcessor : public DMLPackageProcessor
{

public:
	CommandPackageProcessor(BRM::DBRM* aDbrm, uint32_t sid) : DMLPackageProcessor(aDbrm, sid){}
    /** @brief process an CommandDMLPackage
      *
      * @param cpackage the CommandDMLPackage to process
      */
    EXPORT DMLResult processPackage(dmlpackage::CalpontDMLPackage& cpackage);

protected:

private:
	void viewTableLock(const dmlpackage::CalpontDMLPackage& cpackage,
		DMLResult& result );
	void clearTableLock(uint64_t uniqueId,
		const dmlpackage::CalpontDMLPackage& cpackage,
		DMLResult& result );
	void establishTableLockToClear(uint64_t tableLockID,
		BRM::TableLockInfo& lockInfo);

	// Tracks active cleartablelock commands by storing set of table lock IDs
	static std::set<uint64_t> fActiveClearTableLockCmds;
	static boost::mutex       fActiveClearTableLockCmdMutex;
};

}

#undef EXPORT

#endif //COMMANDPACKAGEPROCESSOR_H

