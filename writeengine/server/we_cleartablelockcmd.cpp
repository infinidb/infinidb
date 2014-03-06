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

/*****************************************************************************
 * $Id: we_cleartablelockcmd.cpp 4450 2013-01-21 14:13:24Z rdempsey $
 *
 ****************************************************************************/
#include "we_cleartablelockcmd.h"

#include <iostream>
#include <stdexcept>

#include "we_bulkrollbackmgr.h"
#include "we_confirmhdfsdbfile.h"

#include "IDBPolicy.h"

namespace WriteEngine
{

//------------------------------------------------------------------------------
// Process a bulk rollback request based on input from the specified input
// bytestream object.
//------------------------------------------------------------------------------
int WE_ClearTableLockCmd::processRollback(
	messageqcpp::ByteStream& bs,
	std::string& errMsg)
{
	uint8_t rc = 0;
	errMsg.clear();

	try {
		uint32_t    tableOID;
		uint64_t    tableLockID;
		std::string tableName;
		std::string appName;

		// May want to eventually comment out this logging to stdout,
		// but it shouldn't hurt to keep in here.
		std::cout << "ClearTableLockCmd::processRollback for " << fUserDesc;
		bs >> tableLockID;
		std::cout << ": tableLock-"<< tableLockID;

		bs >> tableOID;
		std::cout << "; tableOID-" << tableOID;

		bs >> tableName;
		std::cout << "; table-"    << tableName;

		bs >> appName;
		std::cout << "; app-"      << appName << std::endl;

		int we_rc = fWEWrapper.bulkRollback(
			tableOID,
			tableLockID,
			tableName,
			appName,
			false, // no extra debug logging to the console
			errMsg );

		if (we_rc != NO_ERROR)
			rc = 2;
	}
	catch(std::exception& ex)
	{
		std::cout << "ClearTableLockCmd::Rollback exception-" << ex.what() <<
			std::endl;
		errMsg = ex.what();
		rc     = 1;
	}

	return rc;
}

//------------------------------------------------------------------------------
// Process a bulk rollback cleanup request based on input from the specified
// input bytestream object.
// Optionally, an additional flag is appended to the message to indicate
// whether HDFS db file *.orig db files are to be cleaned up as well.
// We only need this for the "successful" case, as the bulk rollback takes
// care of db file cleanup in the "unsuccessful" case.
//------------------------------------------------------------------------------
int WE_ClearTableLockCmd::processCleanup(
	messageqcpp::ByteStream& bs,
	std::string& errMsg)
{
	uint8_t rc = 0;
	errMsg.clear();

	try {
		uint32_t tableOID;

		// May want to eventually comment out this logging to stdout,
		// but it shouldn't hurt to keep in here.
		std::cout << "ClearTableLockCmd::processCleanup for " << fUserDesc;
		bs >> tableOID;
		std::cout << ": tableOID-" << tableOID << std::endl;

		// On an HDFS system, this is where we delete any DB files
		// (ex: *.orig) that we no longer need.
		if ((idbdatafile::IDBPolicy::useHdfs()) &&
			(bs.length() >= sizeof(messageqcpp::ByteStream::byte)))
		{
			messageqcpp::ByteStream::byte deleteHdfsTempDbFiles;
			bs >> deleteHdfsTempDbFiles;
			if (deleteHdfsTempDbFiles)
			{
				std::string endDbErrMsg;
				ConfirmHdfsDbFile confirmHdfs;

                // We always pass "true", as this only applies to the "success-
                // ful" case.  See comments that precede this function.
				int endRc = confirmHdfs.endDbFileListFromMetaFile(
					tableOID, true, endDbErrMsg);

				// In this case, a deletion error is not fatal.
				// so we don't propagate a bad return code.  We
				// may want to add syslog msg (TBD)
				if (endRc != NO_ERROR)
					std::cout << "Orig db file deletion error: " <<
					endDbErrMsg << std::endl;
			}
		}

		BulkRollbackMgr::deleteMetaFile( tableOID );
	}
	catch(std::exception& ex)
	{
		std::cout << "ClearTableLockCmd::Cleanup exception-" << ex.what() <<
			std::endl;
		errMsg = ex.what();
		rc     = 1;
	}

	return rc;
}

}
