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
* $Id: we_bulkrollbackmgr.cpp 3716 2012-04-03 18:34:00Z dcathey $
*/

#include <sstream>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#ifdef _MSC_VER
#include <psapi.h>
#endif

#include <boost/scoped_ptr.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>

#define WRITEENGINEBULKROLLMGR_DLLEXPORT
#include "we_bulkrollbackmgr.h"
#undef WRITEENGINEBULKROLLMGR_DLLEXPORT

#include "we_define.h"
#include "we_brm.h"
#include "we_config.h"
#include "we_fileop.h"
#include "we_log.h"
#include "we_bulkrollbackfile.h"
#include "we_bulkrollbackfilecompressed.h"
#include "messageids.h"
#include "cacheutils.h"

namespace
{
	const char* COLUMN_REC      = "COLUMN";
	const int   COLUMN_REC_LEN  = 6;
	const char* DSTORE1_REC     = "DSTOR1";
	const int   DSTORE1_REC_LEN = 6;
	const char* DSTORE2_REC     = "DSTOR2";
	const int   DSTORE2_REC_LEN = 6;
	const char* DATA_DIR_SUFFIX = "_data";
}

namespace WriteEngine
{

//------------------------------------------------------------------------------
// BulkRollbackMgr constructor
//
// tableOID - OID of the table to be rolled back.
//------------------------------------------------------------------------------
BulkRollbackMgr::BulkRollbackMgr ( OID tableOID,
	const std::string& tableName,
	const std::string& applName, Log* logger ) :
	fTableOID(tableOID),
	fTableName(tableName),
	fProcessId(0),
	fPendingDctnryStoreOID(0),
	fSysLogger( logging::LoggingID( SUBSYSTEM_ID_WE ) ),
	fDebugConsole( false ),
	fLog(logger),
	fApplName(applName)
#ifdef _MSC_VER
	, fPids(0), fMaxPids(64)
#endif
{
	Config::initConfigCache();
}

//------------------------------------------------------------------------------
// Rolls back the state of the extentmap and database files for the table OID
// specified to the constructor, using the previously written meta-data file.
// Also clears the existing table lock for the table.
//
// returns:
//   NO_ERROR if rollback completed succesfully
//   ERR_TBL_TABLE_HAS_VALID_CPIMPORT_LOCK - cpimport has the table locked
//   ERR_TBL_TABLE_HAS_VALID_DML_DDL_LOCK  - DML/DDL has the table locked
//   ERR_TBL_TABLE_LOCK_NOT_FOUND          - table not locked
//   plus other errors
//------------------------------------------------------------------------------
int BulkRollbackMgr::rollback ( bool rollbackOnly, bool keepMetaFile )
{
	logAMessage( logging::LOG_TYPE_INFO,
		logging::M0084, 0, fApplName );

	int rc = NO_ERROR;

	bool bFileExists = true;
	rc = metaDataFileExists ( bFileExists );
	if (rc != NO_ERROR)
	{
		std::string msgText(fApplName);
		msgText += ". (rollback failed; ";
		msgText += fErrorMsg;
		msgText += ')';
		logAMessage( logging::LOG_TYPE_ERROR,
			logging::M0085, 0, msgText );

		return rc;
	}

	if (!rollbackOnly)
	{
		rc = validateClearTableLock ( );
		if (rc != NO_ERROR)
		{
			std::string msgText(fApplName);
			msgText += ". (rollback failed; ";
			msgText += fErrorMsg;
			msgText += ')';
			logAMessage( logging::LOG_TYPE_ERROR,
				logging::M0085, 0, msgText );

			return rc;
		}
	}

	// Rollback the data
	if ( bFileExists )
	{
		rc = openMetaDataFile ( );
		if (rc != NO_ERROR)
		{
			std::string msgText(fApplName);
			msgText += ". (rollback failed; ";
			msgText += fErrorMsg;
			msgText += ')';
			logAMessage( logging::LOG_TYPE_ERROR,
				logging::M0085, 0, msgText );

			return rc;
		}

		// validate BRM is in read/write mode so that we can update extentmap
		rc = BRMWrapper::getInstance()->isReadWrite();
		if (rc != NO_ERROR)
		{
			WErrorCodes ec;
			std::ostringstream oss;
			oss << "Bulk rollback for table " << fTableName << " (OID-" <<
				fTableOID << ") not performed; " << ec.errorString(rc);
			fErrorMsg = oss.str();

			std::string msgText(fApplName);
			msgText += ". (rollback failed; ";
			msgText += fErrorMsg;
			msgText += ')';
			logAMessage( logging::LOG_TYPE_ERROR,
				logging::M0085, 0, msgText );

			return rc;
		}

		// Call function to:
		// 1. read bulk rollback meta-data file
		// 2. rollback applicable extents from extentmap
		// 3. delete applicable extents from database files
		// 4. reinitialize blocks trailing the HWM block in the last extent of
		//    each segment file
		// ...
		rc = deleteExtents ( );
		if (rc != NO_ERROR)
		{
			std::string msgText(fApplName);
			msgText += ". (rollback failed; ";
			msgText += fErrorMsg;
			msgText += ')';
			logAMessage( logging::LOG_TYPE_ERROR,
				logging::M0085, 0, msgText );

			return rc;
		}

		// Flush the inode cache.
		int flush_rc = BRMWrapper::getInstance()->flushInodeCaches();
		if (flush_rc != 0)
		{
			WErrorCodes ec;
			std::ostringstream oss;
			oss << "Warning: Error flushing inode cache at end of rollback " <<
				"for table " << fTableName << " (OID-" << fTableOID <<
				"); " << ec.errorString(flush_rc);
			if (fLog)
				fLog->logMsg( oss.str(), MSGLVL_WARNING );
			else
				std::cout << oss.str() << std::endl;
		}

		// Notify PrimProc to flush it's cache.  If error occurs, we tell user,
		// but keep going and release the table lock since the rollback itself
		// finished successfully.
		int cache_rc = cacheutils::flushPrimProcCache();
		if (cache_rc != NO_ERROR)
		{
			std::ostringstream oss;
			oss << "Warning: Error flushing PrimProc cache after rolling "
				"back data for table " << fTableName << " (OID-" << fTableOID <<
				");  Will still release table lock.  rc-" << cache_rc;
			if (fLog)
				fLog->logMsg( oss.str(), MSGLVL_WARNING );
			else
				std::cout << oss.str() << std::endl;
		}
	}

	// Release the table lock
	if (!rollbackOnly)
	{
		rc = BRMWrapper::getInstance()->setTableLock(
			fTableOID, 0, fProcessId, fProcessName, false);
		if (rc != NO_ERROR)
		{
			WErrorCodes ec;
			std::ostringstream oss;
			oss << "Error releasing table lock from table " << fTableName <<
				" (OID-" << fTableOID <<
				"); program-" << fProcessName;
			if ( fProcessId > 0 )
				oss << "; pid-" << fProcessId;
			oss << "; " << ec.errorString(rc);
			fErrorMsg = oss.str();

			std::string msgText(fApplName);
			msgText += ". (rollback failed; ";
			msgText += fErrorMsg;
			msgText += ')';
			logAMessage( logging::LOG_TYPE_ERROR,
				logging::M0085, 0, msgText );

			return rc;
		}
	}

	closeMetaDataFile ( keepMetaFile );

	if ( bFileExists )
	{
	    logAMessage( logging::LOG_TYPE_INFO,
		    logging::M0085, 0, fApplName );
	}
    else
    {
		std::string msgText(fApplName);
		msgText += ". (Nothing to rollback)";
		logAMessage( logging::LOG_TYPE_INFO,
			logging::M0085, 0, msgText );
	}

	return rc;
}

//------------------------------------------------------------------------------
// Validate that a table lock exists for fTableOID, and that that table lock can
// be cleared.  If return code is not NO_ERROR, then an error msg explaining
// why the table lock can not be cleared, can be accessed thru getErrorMsg().
//------------------------------------------------------------------------------
int BulkRollbackMgr::validateClearTableLock ( )
{
	int rc = NO_ERROR;

	// See if the table is already locked
	bool lockStatus;
	u_int32_t sid;
	rc = BRMWrapper::getInstance()->getTableLockInfo (
		fTableOID, fProcessId, fProcessName, lockStatus, sid );
	if (rc != NO_ERROR)
	{
		WErrorCodes ec;
		std::ostringstream oss;
		oss << "Error getting table lock info for TableOID-" << fTableName <<
			" (OID-" << fTableOID <<
			"); " << ec.errorString(rc);
		fErrorMsg = oss.str();
		return rc;
	}

	if (lockStatus)
	{
		if (fProcessId > 0)
		{
		/*	if ( lookupProcessStatus( ) )
			{
				// Table is still locked by an active processId and processName,
				// so should not attempt a rollback
				std::ostringstream oss;
				oss << "Table " << fTableName << " (OID-" << fTableOID <<
					") is still locked by active " << fProcessName <<
					" process; pid-" << fProcessId;
				fErrorMsg = oss.str();
				rc = ERR_TBL_TABLE_HAS_VALID_CPIMPORT_LOCK;
			}
			else
			{
				// table lock is no longer valid, so we can perform the
				// rollback and release the table lock
			} */
		}
		else
		{
			// processId is 0; this denotes a DDL/DML table lock;
			// should not attempt a rollback
			std::ostringstream oss;
			oss << "Table " << fTableName << " (OID-" << fTableOID <<
				") is still locked by active " << fProcessName << " process";
			fErrorMsg = oss.str();
			rc = ERR_TBL_TABLE_HAS_VALID_DML_DDL_LOCK;
		}
	}
	else
	{
		// Table is not locked; should not attempt a rollback
		std::ostringstream oss;
		oss << "Table " << fTableName << " (OID-" << fTableOID <<
			") currently has no table lock";
		fErrorMsg = oss.str();
		rc = ERR_TBL_TABLE_LOCK_NOT_FOUND;
	}
	
	return rc;
}

//------------------------------------------------------------------------------
// See if fProcessId is active and that its process name matches fProcessName.
// Returns true if match is found.
//------------------------------------------------------------------------------
bool BulkRollbackMgr::lookupProcessStatus()
{
#ifdef _MSC_VER
	boost::mutex::scoped_lock lk(fPidMemLock);
	if (!fPids)
		fPids = (DWORD*)malloc(fMaxPids * sizeof(DWORD));
	DWORD needed = 0;
	if (EnumProcesses(fPids, fMaxPids * sizeof(DWORD), &needed) == 0)
		return false;
	while (needed == fMaxPids * sizeof(DWORD))
	{
		fMaxPids *= 2;
		fPids = (DWORD*)realloc(fPids, fMaxPids * sizeof(DWORD));
		if (EnumProcesses(fPids, fMaxPids * sizeof(DWORD), &needed) == 0)
			return false;
	}
	DWORD numPids = needed / sizeof(DWORD);
	for (DWORD i = 0; i < numPids; i++)
	{
		if (fPids[i] == fProcessId)
		{
			TCHAR szProcessName[MAX_PATH] = TEXT("<unknown>");

			// Get a handle to the process.
			HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION |
										   PROCESS_VM_READ,
										   FALSE, fPids[i]);
			// Get the process name.
			if (hProcess != NULL)
			{
				HMODULE hMod;
				DWORD cbNeeded;

				if (EnumProcessModules(hProcess, &hMod, sizeof(hMod), &cbNeeded))
					GetModuleBaseName(hProcess, hMod, szProcessName, 
									   sizeof(szProcessName)/sizeof(TCHAR));

				CloseHandle(hProcess);

				if (fProcessName == szProcessName)
					return true;
			}
		}
	}
	return false;
#else
	bool bMatchFound = false;

	std::ostringstream fileName;
	fileName << "/proc/" << fProcessId << "/stat";
	FILE* pFile = fopen( fileName.str().c_str(), "r" );
	if (pFile)
	{
		pid_t pid;
		char  pName[100];

		// Read in process name based on format of /proc/stat file described
		// in "proc" manpage.  Have to allow for pName being enclosed in ().
		if ( fscanf(pFile, "%d%s", &pid, pName) == 2 )
		{
			pName[strlen(pName)-1] = '\0'; // strip trailing ')'
			if (fProcessName == &pName[1])  // skip leading '(' in comparison
			{
				bMatchFound = true;
			}
		}

		fclose( pFile );
	}
	
	return bMatchFound;
#endif
}

//------------------------------------------------------------------------------
// Verify that meta file exists; if it does not, then we assume that the
// original bulk load job did not get far enough to create a meta file, and
// that nothing needs to get rolled back.  So we don't report this as an error.
//------------------------------------------------------------------------------
int BulkRollbackMgr::metaDataFileExists ( bool& bFileExists )
{
	bFileExists = false;
	std::string bulkRollbackPath( Config::getBulkRollbackDir() );

	// construct the full meta-data file path name
	std::ostringstream oss;
	oss << "/" << fTableOID;
	fMetaFileName  = bulkRollbackPath;
	fMetaFileName += oss.str();
	boost::filesystem::path pathName( fMetaFileName );

	try
	{
		if ( !boost::filesystem::exists( pathName ) )
		{
			return NO_ERROR;
		}
	}
	catch (std::exception& ex)
	{
		int errRc = errno;
		std::ostringstream oss;
		oss << "Error finding/opening bulk rollback meta-data file " <<
			fMetaFileName << "; err-" <<
			errRc << "; " << strerror( errRc );
		fErrorMsg = oss.str();

		return ERR_FILE_OPEN;
	}

	bFileExists = true;
	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Open the meta-data file for fTableOID.  File contains information used in
// rolling back the table to a previous state.
//------------------------------------------------------------------------------
int BulkRollbackMgr::openMetaDataFile ( )
{
	fMetaFile.open( fMetaFileName.c_str() );
	if ( fMetaFile.fail() )
	{
		int errRc = errno;
		std::ostringstream oss;
		oss << "Error opening bulk rollback meta-data file " <<
			fMetaFileName << "; err-" <<
			errRc << "; " << strerror( errRc );
		fErrorMsg = oss.str();

		return ERR_FILE_OPEN;
	}

	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Close and delete the meta-data file used in rolling back fTableOID.
//------------------------------------------------------------------------------
void BulkRollbackMgr::closeMetaDataFile ( bool keepMetaFile )
{
	if (fMetaFile.is_open())
		fMetaFile.close( );

	if (!keepMetaFile)
	{
		unlink( fMetaFileName.c_str() );

		// Unlink corresponding tmp file created by RBMetaWriter.
		std::string tmpMetaFileName = fMetaFileName;
		tmpMetaFileName += ".tmp";
		unlink( tmpMetaFileName.c_str() );

		deleteSubDir();
	}
}

//------------------------------------------------------------------------------
// Delete the subdirectory used to backup data needed for rollback.
//------------------------------------------------------------------------------
void BulkRollbackMgr::deleteSubDir( )
{
	std::string bulkRollbackSubPath( fMetaFileName );
	bulkRollbackSubPath += DATA_DIR_SUFFIX;
	boost::filesystem::path dirPath(bulkRollbackSubPath);

	// Delete bulk rollback data subdirectory
	try
	{
		boost::filesystem::remove_all(dirPath);
	}
	catch (std::exception& ex)
	{
		std::string boostErrString = ex.what();
		std::ostringstream oss;
		oss << "Warning: Error deleting bulk rollback data subdirectory " <<
			bulkRollbackSubPath << "; " << boostErrString;
		if (fLog)
			fLog->logMsg( oss.str(), MSGLVL_WARNING );
		else
			std::cout << oss.str() << std::endl;
	}
}

//------------------------------------------------------------------------------
// Function that drives the rolling back or deletion of extents for a given
// database table as specified in a meta-data bulk rollback file.
//------------------------------------------------------------------------------
int BulkRollbackMgr::deleteExtents ( )
{
	const int BUF_SIZE  = 200;
	char  inBuf[ BUF_SIZE ];
	OID   columnOID     = 0;

	std::string emptyText;
	logAMessage( logging::LOG_TYPE_INFO,
		logging::M0072, 0, emptyText );

	// Loop through the records in the meta-data file
	while (fMetaFile.getline( inBuf, BUF_SIZE ))
	{
		if (strncmp(inBuf, COLUMN_REC, COLUMN_REC_LEN) == 0)
		{
			// Process any pending dictionary deletes
			if (fPendingDctnryExtents.size() > 0)
			{
				int rc = deleteDctnryExtents ( );
				if (rc != NO_ERROR)
				{
					return rc;
				}
			}

			int rc = deleteColumnExtents ( inBuf );
			if (rc != NO_ERROR)
			{
				return rc;
			}
		}
		else if ((strncmp(inBuf, DSTORE1_REC, COLUMN_REC_LEN) == 0) ||
				 (strncmp(inBuf, DSTORE2_REC, COLUMN_REC_LEN) == 0))
		{
			if (fPendingDctnryExtents.size() > 0)
			{
				char recType[100];
				int numFields = sscanf(inBuf, "%s %d", recType, &columnOID );
				if (numFields != 2)
				{
					std::ostringstream oss;
					oss << "Invalid record in meta-data file " << fMetaFileName
						<< "; record-<" << inBuf << ">" <<std::endl;
					fErrorMsg = oss.str();

					return ERR_INVALID_PARAM;
				}

				// Process any pending dictionary deletes
				if (columnOID != fPendingDctnryStoreOID)
				{
					int rc = deleteDctnryExtents ( );
					if (rc != NO_ERROR)
					{
						return rc;
					}
				}
			}

			int rc = readMetaDataRecDctnry ( inBuf );
			if (rc != NO_ERROR)
			{
				return rc;
			}
		}
		else
		{
			// ignore unrecognized record type
		}
	} // end of loop through all the records in the meta-data input file

	// Process any pending dictionary deletes
	if (fPendingDctnryExtents.size() > 0)
	{
		int rc = deleteDctnryExtents ( );
		if (rc != NO_ERROR)
		{
			return rc;
		}
	}

	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Read a meta-data dictionary record (DSTOR1 or DSTOR2) from meta-data file.
// Each record specifies the rollback point for a given dbroot, partition,
// segment number, and HWM for a given dictionary store OID.
//
// inBuf - latest dictionary extent record to be parsed from meta-data file
//------------------------------------------------------------------------------
int BulkRollbackMgr::readMetaDataRecDctnry ( const char* inBuf )
{
	char      recType[100];
	OID       dStoreOID;
	u_int32_t dbRootHwm;
	u_int32_t partNumHwm;
	u_int32_t segNumHwm;
	HWM       localHwm;
	int       compressionType = 0; // optional parameter

	sscanf(inBuf, "%s", recType);
	RollbackData rbData;

	// Process DSTORE1 records representing segment files with an HWM
	if ((strncmp(recType, DSTORE1_REC, COLUMN_REC_LEN) == 0))
	{
		int numFields = sscanf(inBuf, "%s %u %u %u %u %u %d",
			recType, &dStoreOID,
			&dbRootHwm, &partNumHwm, &segNumHwm, &localHwm, &compressionType );

		if (numFields < 6)
		{
			std::ostringstream oss;
			oss << "Invalid DSTOR1 record in meta-data file " <<
				fMetaFileName << "; record-<" << inBuf << ">" << std::endl;
			fErrorMsg = oss.str();

			return ERR_INVALID_PARAM;
		}

		rbData.fWithHwm = true;
		rbData.fHwm     = localHwm;
	}

	// Process DSTORE2 records representing segment files w/o HWM; meaning that
	// segment file did not previously exist and can be deleted if it was added
	else
	{
		int numFields = sscanf(inBuf, "%s %u %u %u %u %d",
			recType, &dStoreOID,
			&dbRootHwm, &partNumHwm, &segNumHwm, &compressionType );

		if (numFields < 5)
		{
			std::ostringstream oss;
			oss << "Invalid DSTOR2 record in meta-data file " <<
				fMetaFileName << "; record-<" << inBuf << ">" << std::endl;
			fErrorMsg = oss.str();

			return ERR_INVALID_PARAM;
		}

		rbData.fWithHwm = false;
		rbData.fHwm     = 0;
	}

	rbData.fDbRoot  = dbRootHwm;
	rbData.fPartNum = partNumHwm;
	rbData.fSegNum  = segNumHwm;

	fPendingDctnryExtents.push_back( rbData );

	// OID and compression type should be the same for all store files relating
	// to the same dictionary column, but they are set for each record nonethe-
	// less since the format of the meta data file is a flat file format.
	fPendingDctnryStoreOID = dStoreOID;
	fPendingDctnryStoreCompressionType = compressionType;
	
	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Delete all the column extents (from the extent map and the db files) that
// logically follow the HWM extent contained in inBuf; where inBuf is a
// COLUMN record read from a meta-data bulk rollback file.
//
// inBuf - latest column extent record to be parsed from meta-data file
//------------------------------------------------------------------------------
int BulkRollbackMgr::deleteColumnExtents ( const char* inBuf )
{
	char        recType[100];
	OID         columnOID;
	u_int32_t   dbRootHwm;
	u_int32_t   partNumHwm;
	u_int32_t   segNumHwm;
	HWM         lastLocalHwm;
	int         colTypeInt;
	ColDataType colType;
	char        colTypeName[100];
	u_int32_t   colWidth;
	int         compressionType = 0; // optional parameter
	
	// Read meta-data record
	int numFields = sscanf(inBuf, "%s %u %u %u %u %u %d %s %u %d",
		recType, &columnOID,
		&dbRootHwm, &partNumHwm, &segNumHwm, &lastLocalHwm,
		&colTypeInt, colTypeName, &colWidth, &compressionType );
	colType = (WriteEngine::ColDataType)colTypeInt;
	if (numFields < 9)
	{
		std::ostringstream oss;
		oss << "Invalid COLUMN record in meta-data file " <<
			fMetaFileName << "; record-<" << inBuf << ">" << std::endl;
		fErrorMsg = oss.str();

		return ERR_INVALID_PARAM;
	}

	logAMessage( logging::LOG_TYPE_INFO,
		logging::M0073, columnOID, std::string("(column extent)") );

	// Create the object responsible for restoring the extents in the db files.
	BulkRollbackFile* fileRestorer = 0;
	if (compressionType)
		fileRestorer = new BulkRollbackFileCompressed(this);
	else
		fileRestorer = new BulkRollbackFile(this);
	boost::scoped_ptr<BulkRollbackFile> refBulkRollbackFile(fileRestorer);

	// Delete extents from the extentmap
	std::ostringstream msgText;
	msgText << "Restoring HWM column extent to: partition-" << partNumHwm <<
		"; segment-" << segNumHwm     <<
		"; hwm-"     << lastLocalHwm;
	logAMessage( logging::LOG_TYPE_INFO,
		logging::M0074, columnOID, msgText.str() );

	int rc = BRMWrapper::getInstance()->rollbackColumnExtents (
		columnOID,
		partNumHwm,
		(u_int16_t)segNumHwm,
		lastLocalHwm );
	if (rc != NO_ERROR)
	{
		WErrorCodes ec;
		std::ostringstream oss;
		oss << "Error rolling back column extents from extent map for " <<
			columnOID <<
			"; partition-" << partNumHwm <<
			"; segment-"   << segNumHwm  <<
			"; hwm-"       << lastLocalHwm <<
			"; "           << ec.errorString(rc);
		fErrorMsg = oss.str();
		return ERR_BRM_BULK_RB_COLUMN;
	}

	// Determine the exact rollback point for the extents we are rolling back to
	const unsigned BLKS_PER_EXTENT =
		(BRMWrapper::getInstance()->getExtentRows() * colWidth)/BYTE_PER_BLOCK;
	u_int32_t lastBlkOfCurrStripe = 0;
	u_int32_t lastBlkOfPrevStripe = 0;
	if ((lastLocalHwm + 1) <= BLKS_PER_EXTENT)
	{
		lastBlkOfCurrStripe = BLKS_PER_EXTENT - 1;
	}
	else
	{
		lastBlkOfPrevStripe = lastLocalHwm -
			(lastLocalHwm % BLKS_PER_EXTENT) - 1;
		lastBlkOfCurrStripe = lastBlkOfPrevStripe + BLKS_PER_EXTENT;
	}

	// Figure out DBRoot for segment 0 so we can loop thru segment files
	int dbRootKount = Config::DBRootCount();
	int firstDbRoot = (int)dbRootHwm - (segNumHwm % dbRootKount);
	if (firstDbRoot <= 0)
		firstDbRoot += dbRootKount;

	// Delete extents from the database files
	u_int32_t maxSegCount = Config::getFilesPerColumnPartition();

	u_int32_t partNum = partNumHwm;
	bool continueFlag = true;

	// Loop through all partitions (starting with the HWM partition partNumHwm),
	// deleting or restoring applicable extents.  continueFlag will stay true
	// until we can no longer find any column segment files to rollback.
	while ( continueFlag )
	{
		int dbRoot = firstDbRoot;

		for (u_int32_t segNum=0; segNum<maxSegCount; segNum++)
		{
			if ( partNum == partNumHwm )
			{
				if ( segNum < segNumHwm )
				{
					int rc = fileRestorer->truncateSegmentFile ( columnOID,
						dbRoot,
						partNum,
						segNum,
						(lastBlkOfCurrStripe + 1) );
					if (rc != NO_ERROR)
					{
						return rc;
					}
				} // end of (segNum < segNumHwm)

				else if ( segNum > segNumHwm )
				{
					if (lastBlkOfPrevStripe > 0)
					{
						int rc = fileRestorer->truncateSegmentFile ( columnOID,
							dbRoot,
							partNum,
							segNum,
							(lastBlkOfPrevStripe + 1) );
						if (rc != NO_ERROR)
						{
							return rc;
						}
					}
					// lastBlkOfPrevStripe = 0, means there was no previous
					// stripe in this partition.  The HWM block was in the
					// first stripe.  In this case we can delete any segment
					// files added to this partition that follow segNumHwm.
					else
					{
						int rc = fileRestorer->deleteSegmentFile ( columnOID,
							true,	// column segment file
							dbRoot,
							partNum,
							segNum,				
							continueFlag );
						if (rc != NO_ERROR)
						{
							return rc;
						}
					}
				} // end of (segNum > segNumHwm)

				else // segNum == segNumHwm
				{
					// Reinit last extent and truncate the remainder,
					// starting with the next block following the HWM block.
					rc = fileRestorer->reInitTruncColumnExtent ( columnOID,
						dbRoot,
						partNum,
						segNum,
						(lastLocalHwm + 1),
						(lastBlkOfCurrStripe - lastLocalHwm),
						colType,
						colWidth );
					if (rc != NO_ERROR)
					{
						return rc;
					}
				} // end of (segNum == segNumHwm)
			}
			else // ( partNum > partNumHwm )
			{
				// Delete any files added to subsequent partitions
				int rc = fileRestorer->deleteSegmentFile ( columnOID,
					true,	// column segment file
					dbRoot,
					partNum,
					segNum,				
					continueFlag );
				if (rc != NO_ERROR)
				{
					return rc;
				}
			}

			if (!continueFlag)
				break;

			dbRoot++;
			if (dbRoot > dbRootKount)
				dbRoot = 1;

		} // loop thru all the potential segment files in a partition

		partNum++;

	} // end of loop to go thru all partitions till we find last segment file

	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Delete all the dictionary extents (from the extent map and the db files) that
// logically follow the extents contained in fPendingDctnryExtents; where
// fPendingDctnryExtents is a vector of DSTOR1 and DSTOR2 records (for a
// specific column OID), read from a meta-data bulk rollback file.
//------------------------------------------------------------------------------
int BulkRollbackMgr::deleteDctnryExtents ( )
{
	logAMessage( logging::LOG_TYPE_INFO,
		logging::M0073,
		fPendingDctnryStoreOID,
		std::string("(dictionary store extent)") );

	if (fPendingDctnryExtents.size() == 0)
		return NO_ERROR;

	if (fPendingDctnryExtents.size() != Config::getFilesPerColumnPartition())
	{
		std::ostringstream oss;
		oss<< "Invalid no. of seg files (" << fPendingDctnryExtents.size() <<
			") specified for rolled back dictionary extents for " <<
			fPendingDctnryStoreOID;
		fErrorMsg = oss.str();
		return ERR_INVALID_PARAM;
	}

	std::vector<BRM::HWM_t> hwms;

	// Build up list of HWM's to be sent to DBRM for extentmap rollback
	for (unsigned i=0; i<fPendingDctnryExtents.size(); i++)
	{
		if ( !fPendingDctnryExtents[i].fWithHwm )
			break;

		hwms.push_back( fPendingDctnryExtents[i].fHwm );
	}

	// Delete extents from the extentmap using hwms vector
	std::ostringstream msgText;
	msgText << "Restoring HWM dictionary store extent to: partition-" <<
		fPendingDctnryExtents[0].fPartNum << "; HWM(s): ";
	for (unsigned int k=0; k<hwms.size(); k++)
	{
		if (k > 0)
			msgText << ", ";
		msgText << hwms[k];
	}

	logAMessage( logging::LOG_TYPE_INFO,
		logging::M0074, fPendingDctnryStoreOID, msgText.str() );

	// Create the object responsible for restoring the extents in the db files.
	BulkRollbackFile* fileRestorer = 0;
	if (fPendingDctnryStoreCompressionType)
		fileRestorer = new BulkRollbackFileCompressed(this);
	else
		fileRestorer = new BulkRollbackFile(this);
	boost::scoped_ptr<BulkRollbackFile> refBulkRollbackFile(fileRestorer);

	int rc = BRMWrapper::getInstance()->rollbackDictStoreExtents (
		fPendingDctnryStoreOID,
		fPendingDctnryExtents[0].fPartNum,
		hwms );
	if (rc != NO_ERROR)
	{
		WErrorCodes ec;
		std::ostringstream oss;
		oss<< "Error rolling back dictionary extents from extent map for "<<
			fPendingDctnryStoreOID <<
			"; partNum-" << fPendingDctnryExtents[0].fPartNum <<
			"; "         << ec.errorString(rc);
		fErrorMsg = oss.str();
		return ERR_BRM_BULK_RB_DCTNRY;
	}

	// Assign constants used later in calculating exact rollback point
	const unsigned COL_WIDTH       = 8;
	const unsigned ROWS_PER_EXTENT = BRMWrapper::getInstance()->getExtentRows();
	const unsigned BLKS_PER_EXTENT =
		(ROWS_PER_EXTENT * COL_WIDTH)/BYTE_PER_BLOCK;

	// Delete extents from the database files
	u_int32_t maxSegCount = Config::getFilesPerColumnPartition();

	u_int32_t partNum = fPendingDctnryExtents[0].fPartNum;
	bool continueFlag = true;

	// Loop through all partitions (starting with the HWM partition
	// fPartNum), deleting or restoring applicable extents.
	// continueFlag will stay true until we can no longer find
	// any dictionary store segment files to rollback.
	while ( continueFlag )
	{
		for (u_int32_t segNum=0; segNum<maxSegCount; segNum++)
		{
			if ( partNum == fPendingDctnryExtents[0].fPartNum )
			{
				if ( fPendingDctnryExtents[segNum].fWithHwm )
				{
					HWM hwm = fPendingDctnryExtents[segNum].fHwm;

					// Determine the exact rollback point for the extent
					// we are rolling back to
					u_int32_t lastBlkOfCurrStripe = hwm - 
						(hwm % BLKS_PER_EXTENT) + BLKS_PER_EXTENT - 1;

					// Reinit last extent and truncate the remainder,
					// starting with the next block following the HWM block.
					rc = fileRestorer->reInitTruncDctnryExtent (
						fPendingDctnryStoreOID,
						fPendingDctnryExtents[segNum].fDbRoot,
						partNum,
						segNum,
						(hwm + 1),
						(lastBlkOfCurrStripe - hwm));
					if (rc != NO_ERROR)
					{
						return rc;
					}
				}
				else // don't keep this segment file
				{
					int rc = fileRestorer->deleteSegmentFile (
						fPendingDctnryStoreOID,
						false,	// not a column segment file
						fPendingDctnryExtents[segNum].fDbRoot,
						partNum,
						segNum,				
						continueFlag );
					if (rc != NO_ERROR)
					{
						return rc;
					}
				}
			}
			else // ( partNum > fPendingDctnryExtents[0].fPartNum )
			{
				// don't keep this segment file
				int rc = fileRestorer->deleteSegmentFile (
					fPendingDctnryStoreOID,
					false,	// not a column segment file
					fPendingDctnryExtents[segNum].fDbRoot,
					partNum,
					segNum,				
					continueFlag );
				if (rc != NO_ERROR)
				{
					return rc;
				}
			}

			if (!continueFlag)
				break;

		} // loop thru all the potential segment files in a partition

		partNum++;

	} //end of loop to go thru all partitions till we find last segment file

	fPendingDctnryExtents.clear ( );

	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Log a message to syslog.  columnOID and text are used depending on the msgId.
//
// logType   - type of message (debug, critical, etc)
// msgId     - message ID
// columnOID - column OID associated with this rollback message
// text      - message text
//------------------------------------------------------------------------------
void BulkRollbackMgr::logAMessage (
	logging::LOG_TYPE           logType,
	logging::Message::MessageID msgId,
	OID                         columnOID,
	const std::string&          text )
{
	logging::Message m( msgId );
	logging::Message::Args args;

	std::ostringstream ossTbl;
	ossTbl << fTableName << " (OID-" << fTableOID << ")";
	args.add( ossTbl.str() );

	if (msgId >= logging::M0073)
	{
		if (msgId <= logging::M0075)
			args.add( (uint64_t)columnOID );
		args.add( text );
	}
	m.format( args );

	// Log to syslog
	// Note that WARNING, ERROR and CRITICAL are logged to INFO as well as
	// their respective log files, so that the message will appear in context
	// with all the other INFO msgs used to track the flow of the rollback.
	switch (logType)
	{
		case logging::LOG_TYPE_DEBUG:
		{
			fSysLogger.logDebugMessage( m );
			break;
		}

		case logging::LOG_TYPE_INFO:
		{
			fSysLogger.logInfoMessage( m );
			break;
		}

		case logging::LOG_TYPE_WARNING:
		{
			fSysLogger.logWarningMessage( m );
			fSysLogger.logInfoMessage   ( m );
			break;
		}

		case logging::LOG_TYPE_ERROR:
		{
			fSysLogger.logErrorMessage( m );
			fSysLogger.logInfoMessage ( m );
			break;
		}

		default: // LOG_TYPE_CRITICAL
		{
			fSysLogger.logCriticalMessage( m );
			fSysLogger.logInfoMessage    ( m );
			break;
		}
	}

	// If fLog is defined then log to there, else log to cout.
	// Currently log msg0074 and msg0075 to console only if debug is enabled
	switch (msgId)
	{
		// Log the name of the table to be rolled back or restored
		case logging::M0072:
		{
			if (fLog)
			{
				std::ostringstream oss;
				oss << "Rolling back extents for table " <<
					fTableName << " (OID-" << fTableOID << ")";
				fLog->logMsg( oss.str(), MSGLVL_INFO2 );
			}
			else
			{
				std::cout << "Rolling back extents for table " <<
					fTableName << " (OID-" << fTableOID << ")" << std::endl;
			}
			break;
		}

		// Log the name of the table and column to be rolled back or restored
		case logging::M0073:
		{
			if (fLog)
			{
				std::ostringstream oss;
				oss << "Rolling back extents for table " <<
					fTableName << " (OID-" << fTableOID <<
					"); column " << columnOID << "; " << text;
				fLog->logMsg( oss.str(), MSGLVL_INFO2 );
			}
			else
			{
				std::cout << "Rolling back extents for table " <<
					fTableName << " (OID-" << fTableOID <<
					"); column " << columnOID << "; " << text << std::endl;
			}
			break;
		}

		// Log the rolling back of extent(s) from the extent map
		case logging::M0074:
		{
			if (fLog)
			{
				std::ostringstream oss;
				oss << "Rolling back extent map for table " <<
					fTableName << " (OID-" << fTableOID <<
					"); column " << columnOID << "; " << text;
				fLog->logMsg( oss.str(), MSGLVL_INFO2 );
			}
			else
			{
				if ( fDebugConsole )
				{
					std::cout << "Rolling back extent map for table " <<
						fTableName << " (OID-" << fTableOID <<
						"); column " << columnOID << "; " << text << std::endl;
				}
			}
			break;
		}

		// Log the rolling back of extent(s) from the DB 
		case logging::M0075:
		{
			if (fLog)
			{
				std::ostringstream oss;
				oss << "Rolling back db file for table " <<
					fTableName << " (OID-" << fTableOID <<
					"); column " << columnOID << "; " << text;
				fLog->logMsg( oss.str(), MSGLVL_INFO2 );
			}
			else
			{
				if ( fDebugConsole )
				{
					std::cout << "Rolling back db file for table " <<
						fTableName << " (OID-" << fTableOID <<
						"; column " << columnOID << "; " << text << std::endl;
				}
			}
			break;
		}

		// Log the start of a bulk rollback
		case logging::M0084:
		{
			if (fLog)
			{
				std::ostringstream oss;
				oss << "Starting bulk rollback for table " <<
					fTableName << " (OID-" << fTableOID <<
					") in " << text;
				fLog->logMsg( oss.str(), MSGLVL_INFO2 );
			}
			else
			{
				std::cout << "Starting bulk rollback for table " <<
					fTableName << " (OID-" << fTableOID <<
					") in " << text << std::endl;
			}
			break;
		}

		// Log the end of a bulk rollback
		case logging::M0085:
		{
			if (fLog)
			{
				std::ostringstream oss;
				oss << "Ending bulk rollback for table " <<
					fTableName << " (OID-" << fTableOID <<
					") in " << text;
				fLog->logMsg( oss.str(), MSGLVL_INFO2 );
			}
			else
			{
				std::cout << "Ending bulk rollback for table " <<
					fTableName << " (OID-" << fTableOID <<
					") in " << text << std::endl;
			}
			break;
		}

		default:
		{
			break;
		}
	}
}

} //end of namespace
