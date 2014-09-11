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
* $Id: we_bulkrollbackmgr.h 3603 2012-03-06 16:39:32Z dcathey $
*/

/** @file
 * Contains class to clear a database table lock, and rolls back extents
 * based on HWM meta data saved by a bulk load.
 */

#ifndef WE_BULKROLLBACKMGR_H_
#define WE_BULKROLLBACKMGR_H_

#ifdef _MSC_VER
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <boost/thread/mutex.hpp>
#endif
#include <string>
#include <fstream>
#include <vector>

#include "we_type.h"
#include "messagelog.h"
#include "messageobj.h"

#if defined(_MSC_VER) && defined(WRITEENGINEBULKROLLMGR_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace WriteEngine
{
	class Log;

//------------------------------------------------------------------------------
/** @brief Class to clear a database table lock, and rolls back extents
 *  based on HWM meta data saved by a bulk load.
 */
//------------------------------------------------------------------------------
class BulkRollbackMgr
{
public:
	/**
	 * @brief BulkRollbackMgr constructor
	 * @param tableOID table to be rolled back.
	 * @param tableName name of table associated with tableOID
	 * @param applName application that is driving this bulk rollback
	 */
	EXPORT BulkRollbackMgr(OID tableOID,
                           const std::string& tableName,
                           const std::string& applName,
                           Log* logger=0);

#ifdef _MSC_VER
	EXPORT ~BulkRollbackMgr() { if (fPids) free(fPids); }
#endif
	/**
	 * @brief Clear table lock and rollback extents for fTableOID
	 * @param rollbackOnly requests rollback w/o clearing table lock
	 * @param keepMetaFile controls whether the meta data file is deleted
	 * @return NO_ERROR upon success
	 *         ERR_TBL_TABLE_HAS_VALID_CPIMPORT_LOCK - locked by cpimport,
	 *         ERR_TBL_TABLE_HAS_VALID_DML_DDL_LOCK  - locked by DDL/DML,
	 *         ERR_TBL_TABLE_LOCK_NOT_FOUND          - table has no lock,
	 *         other errors such as a BRM error are possible
	 */
	EXPORT int rollback ( bool rollbackOnly, bool keepMetaFile );

	/**
	 * @brief Accessor to any error msg related to a bad return code.
	 * @return error message if rollback rejected or failed.
	 */
	const std::string& getErrorMsg( ) const { return fErrorMsg; }

	/**
	 * @brief Save error msg for later reference.
	 */
	void setErrorMsg ( const std::string& errMsg ) { fErrorMsg = errMsg; }

	/**
	 * @brief Accessor to the name of the meta file we are processing
	 */
	const std::string& getMetaFileName() const { return fMetaFileName; }

	/**
	 * @brief Mutator to enable/disable debug logging to console.
	 */
	const void setDebugConsole ( bool debug ) { fDebugConsole = debug; }

	/**
	 * @brief Log the specified message.
	 * @param logType   type of message to be logged
	 * @param msgId     message id to be used
	 * @param columnOID column OID
	 * @param text      message text to be logged
	 */
	void logAMessage          ( logging::LOG_TYPE   logType, //log a message
                                logging::Message::MessageID msgId,
                                OID                 columnOID,
                                const std::string&  text );

private:
#ifdef _MSC_VER
	//Needs proper copy ctors on Windows
	BulkRollbackMgr(const BulkRollbackMgr& rhs);
	BulkRollbackMgr& operator=(const BulkRollbackMgr& rhs);
#endif
	// Structure used to store info for the list of dictionary store
	// segment files in the last partition.
	/**
	 * @brief Structure used to store info for the list of dictionary
	 * store segment files in the last partition.
	 */
	struct RollbackData
	{
		u_int32_t	fDbRoot;
		u_int32_t	fPartNum;
		u_int32_t	fSegNum;
		HWM			fHwm;
		bool		fWithHwm;
	};

	int deleteColumnExtents   ( const char* inBuf ); // delete col extents
	int deleteDctnryExtents   ( ); // delete dictionary store extents
	int deleteExtents         ( ); // function that drives extent deletion
	void deleteSubDir         ( ); // delete subdirectory used for backup chunks
	void closeMetaDataFile    ( bool keepMetaFile ); //close/delete metafile
	bool lookupProcessStatus  ( ); // are fProcessId/fProcessName active
	int metaDataFileExists    ( bool& exists ); // does meta-data file exists
	int openMetaDataFile      ( ); // open meta-data file with HWM info
	int readMetaDataRecDctnry ( const char* inBuf );//read meta-data dct rec
	int validateClearTableLock( ); // verify fTableOID lock can be cleared

	// Data members
	OID           fTableOID;	// table to be rolled back
	std::string   fTableName;	// name of table associated with fTableOID
	u_int32_t     fProcessId;	// pid associated with current table lock
	std::string   fProcessName;	// processName associated with fProcessId
	std::ifstream fMetaFile;	// meta data file we are reading
	std::string   fMetaFileName;// name of meta data file
	std::string   fErrorMsg;
	unsigned char fDctnryHdr[DCTNRY_HEADER_SIZE]; // empty dctnry store blk

	// Dictionary store extents for an OID are read in and managed as a
	// group.  The following data members are used to collect this info.
	OID           fPendingDctnryStoreOID;  // OID of pending dctnry extents
	int           fPendingDctnryStoreCompressionType; // Dctnry compression type
	std::vector<RollbackData> fPendingDctnryExtents;

	logging::MessageLog fSysLogger; // Used for syslogging
	bool          fDebugConsole;    // control debug logging to console
	Log*          fLog;             // optional logger object
	std::string   fApplName;        // application initiating the bulk rollback
#ifdef _MSC_VER
	boost::mutex fPidMemLock;
	DWORD* fPids;
	DWORD fMaxPids;
#endif
};

} //end of namespace

#undef EXPORT

#endif // WE_BULKROLLBACKMGR_H_
