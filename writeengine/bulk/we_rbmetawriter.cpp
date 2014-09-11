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
* $Id: we_rbmetawriter.cpp 3603 2012-03-06 16:39:32Z dcathey $
*/

#include "we_rbmetawriter.h"

#include <cerrno>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>

#include "we_config.h"
#include "we_convertor.h"
#include "we_define.h"
#include "we_fileop.h"
#include "we_log.h"
#include "idbcompress.h"
using namespace compress;

namespace
{
const char* DATA_DIR_SUFFIX = "_data";

	//--------------------------------------------------------------------------
	// Local Function that prints contents of an RBChunkInfo object
	//--------------------------------------------------------------------------
	std::ostream& operator<<(std::ostream& os,
		const WriteEngine::RBChunkInfo& chk)
	{
		os << "OID-"    << chk.fOid       <<
			"; DBRoot-" << chk.fDbRoot    <<
			"; Part-"   << chk.fPartition <<
			"; Seg-"    << chk.fSegment   <<
			"; HWM-"    << chk.fHwm;

		return os;
	}
}

namespace WriteEngine
{

//------------------------------------------------------------------------------
// Compare function used for set of RBChunkInfo objects.
//------------------------------------------------------------------------------
bool RBChunkInfoCompare::operator()
	(const RBChunkInfo& lhs, const RBChunkInfo& rhs) const
{
	if (lhs.fOid < rhs.fOid) {
		return true;
	}

	if ((lhs.fOid==rhs.fOid) && (lhs.fSegment < rhs.fSegment)) {
		return true;
	}

	return false;
}

//------------------------------------------------------------------------------
// RBMetaWriter constructor
//------------------------------------------------------------------------------
RBMetaWriter::RBMetaWriter ( Log* logger ) : fLog(logger), fCreatedSubDir(false)
{
}

//------------------------------------------------------------------------------
// Initialize this meta data file object.
//------------------------------------------------------------------------------
int RBMetaWriter::init (
	OID tableOID,
	const std::string& tableName )
{
	fTableOID  = tableOID;
	fTableName = tableName;

	std::string bulkRollbackPath( Config::getBulkRollbackDir() );

	// Construct the filename; use a temporary file name until we are complete,
	// at which time we will rename the temporary file to the desired name.
	std::ostringstream oss;
	oss << "/" << fTableOID;
	fMetaFileName     = bulkRollbackPath;
	fMetaFileName    += oss.str();
	fTmpMetaFileName  = fMetaFileName;
	fTmpMetaFileName += ".tmp";

	// Delete any files that collide with the filenames we intend to use
	unlink( fMetaFileName.c_str() );
	unlink( fTmpMetaFileName.c_str() );

	// Clear out any data subdirectory
	RETURN_ON_ERROR( deleteSubDir() );

	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Open a meta data file to save info about the specified table OID.
//------------------------------------------------------------------------------
int RBMetaWriter::openFile ( )
{
	std::string bulkRollbackPath( Config::getBulkRollbackDir() );
	boost::filesystem::path pathDir(bulkRollbackPath);

	// create bulk rollback directory if it does not exist
	try
	{
		if ( !boost::filesystem::exists( pathDir ) )
		{
			std::string boostErrString;
			try
			{
				boost::filesystem::create_directories(pathDir);
			}
			catch (std::exception& ex)
			{
				// ignore exception for now; we may have just had a
				// race condition where 2 jobs were creating dirs.
				boostErrString = ex.what();
			}

			if ( !boost::filesystem::exists( pathDir ) )
			{
				std::ostringstream oss;
				oss << "Error creating bulk rollback directory(1) " <<
					bulkRollbackPath << "; " << boostErrString;
				fLog->logMsg( oss.str(), ERR_DIR_CREATE, MSGLVL_ERROR );

				return ERR_DIR_CREATE;
			}
		}
	}
	catch (std::exception& ex)
	{
		std::ostringstream oss;
		oss << "Error creating bulk rollback directory(2) " <<
			bulkRollbackPath << "; " << ex.what();
		fLog->logMsg( oss.str(), ERR_DIR_CREATE, MSGLVL_ERROR );

		return ERR_DIR_CREATE;
	}

	if (!boost::filesystem::is_directory(pathDir) )
	{
		std::ostringstream oss;
		oss << "Error creating bulk rollback directory " <<
			bulkRollbackPath << "; path already exists as non-directory" <<
			std::endl;
		fLog->logMsg( oss.str(), ERR_DIR_CREATE, MSGLVL_ERROR );

		return ERR_DIR_CREATE;
	}

	// Open the file; use a temporary file name until we are complete, at
	// which time we will rename the temporary file to the desired name.
	fMetaDataFile.open( fTmpMetaFileName.c_str() );
	if ( fMetaDataFile.fail() )
	{
		int errRc = errno;
		std::ostringstream oss;
		std::string eMsg;
		Convertor::mapErrnoToString(errRc, eMsg);
		oss << "Error opening bulk rollback file " <<
			fTmpMetaFileName << "; " << eMsg;
		fLog->logMsg( oss.str(), ERR_FILE_OPEN, MSGLVL_ERROR );

		return ERR_FILE_OPEN;
	}

	// Write header information to the file
	fMetaDataFile <<
		"# VERSION: 2"             << std::endl <<
		"# PID:    " << ::getpid() << std::endl <<
		"# TABLE:  " << fTableName << std::endl <<
		"# COLUMN: oid,dbroot,part,seg,lastLocalHWM,type,typename,width,comp" <<
			std::endl <<
		"# DSTOR1: oid,dbroot,part,seg,localHWM,comp" << std::endl <<
		"# DSTOR2: oid,dbroot,part,seg,comp" << std::endl;

	// Clear out any data subdirectory
    // This is redundant because init() also calls deleteSubDir(), but it can't
    // hurt to call twice.  We "really" want ot make sure we start with a clean
    // slate (no leftover backup chunk files from a previous import job).
	RETURN_ON_ERROR( deleteSubDir() );

	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Close the currently open meta data file, and only then do we rename from
// the temporary to the permanent file name.
//------------------------------------------------------------------------------
int RBMetaWriter::closeFile ( bool saveFile )
{
	fMetaDataFile.close();

	if (saveFile)
	{
		if ( rename(fTmpMetaFileName.c_str(), fMetaFileName.c_str()) )
		{
			int errRc = errno;
			std::ostringstream oss;
			std::string eMsg;
			Convertor::mapErrnoToString(errRc, eMsg);
			oss << "Error renaming meta data file-" <<
				fTmpMetaFileName << "; will be deleted; " << eMsg;
			fLog->logMsg( oss.str(), ERR_METADATABKUP_FILE_RENAME,
				MSGLVL_ERROR );

			unlink( fMetaFileName.c_str() );
			unlink( fTmpMetaFileName.c_str() );

			return ERR_METADATABKUP_FILE_RENAME;
		}
	}

	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Delete the meta data file for the specified table OID.
//------------------------------------------------------------------------------
void RBMetaWriter::deleteFile ( )
{
	if (!fMetaFileName.empty())
	{
		unlink( fMetaFileName.c_str() );
		unlink( fTmpMetaFileName.c_str() );

		deleteSubDir(); // okay to ignore return code in this case
	}
}

//------------------------------------------------------------------------------
// Save the specified meta data for columnOID.
//------------------------------------------------------------------------------
int RBMetaWriter::writeColumnMetaData (
	OID                columnOID,
	uint16_t	       dbRoot,
	uint32_t	       partition,
	uint16_t	       segment,
	HWM                lastLocalHwm,
	ColDataType        colType,
	const std::string& colTypeName,
	int                colWidth,
	int                compressionType )
{
	fMetaDataFile    << "COLUMN: " <<
		columnOID    << ' ' <<
		dbRoot       << ' ' <<
		partition    << ' ' <<
		segment      << ' ' <<
		lastLocalHwm << ' ' <<
		colType      << ' ' <<
		colTypeName  << ' ' <<
		colWidth;
	if (compressionType)
		fMetaDataFile << ' ' << compressionType << ' ';
	fMetaDataFile    << std::endl;

	// If column is compressed, then create directory for storing HWM chunks
	if (compressionType)
	{
		if (!fCreatedSubDir)
		{
			return createSubDir( );
		}
	}

	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Save the specified meta data for dictionaryStoreOID.
//------------------------------------------------------------------------------
void RBMetaWriter::writeDictionaryStoreMetaData (
	OID      dictionaryStoreOID,
	uint16_t dbRoot,
	uint32_t partition,
	uint16_t segment,
	HWM      localHwm,
	int      compressionType )
{
	fMetaDataFile          << "DSTOR1: " <<
		dictionaryStoreOID << ' ' <<
		dbRoot             << ' ' <<
		partition          << ' ' <<
		segment            << ' ' <<
		localHwm;
	if (compressionType)
		fMetaDataFile << ' ' << compressionType << ' ';
	fMetaDataFile << std::endl;

	// Save dictionary meta data for later use in backing up the HWM chunks
	if (compressionType)
	{
		RBChunkInfo chunkInfo(
			dictionaryStoreOID, dbRoot, partition, segment, localHwm);
		fRBChunkDctnrySet.insert( chunkInfo );

		if (fLog->isDebug( DEBUG_1 ))
			printDctnryChunkList(chunkInfo, "after adding ");
	}
}

//------------------------------------------------------------------------------
// Save a meta data marker for the specified dictionaryStoreOID.
// The specified dictionary store segment file does not yet exist in the
// current partition, and should be deleted if a rollback ensues.
//------------------------------------------------------------------------------
void RBMetaWriter::writeDictionaryStoreMetaNoDataMarker (
	OID      dictionaryStoreOID,
	uint16_t dbRoot,
	uint32_t partition,
	uint16_t segment,
	int      compressionType )
{
	fMetaDataFile          << "DSTOR2: " <<
		dictionaryStoreOID << ' ' <<
		dbRoot             << ' ' <<
		partition          << ' ' <<
		segment;
	if (compressionType)
		fMetaDataFile << ' ' << compressionType << ' ';
	fMetaDataFile << std::endl;
}

//------------------------------------------------------------------------------
// Create the subdirectory we will use to backup data needed for rollback.
//------------------------------------------------------------------------------
int RBMetaWriter::createSubDir( )
{
	std::string bulkRollbackSubPath( fMetaFileName );
	bulkRollbackSubPath += DATA_DIR_SUFFIX;
	boost::filesystem::path pathDir(bulkRollbackSubPath);

	try
	{
		boost::filesystem::create_directories(pathDir);
	}
	catch (std::exception& ex)
	{
		std::string boostErrString = ex.what();
		std::ostringstream oss;
		oss << "Error creating bulk rollback data subdirectory " <<
			bulkRollbackSubPath << "; " << boostErrString;
		fLog->logMsg( oss.str(), ERR_DIR_CREATE, MSGLVL_ERROR );

		return ERR_DIR_CREATE;
	}

	fCreatedSubDir = true;

	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Delete the subdirectory used to backup data needed for rollback.
//------------------------------------------------------------------------------
int RBMetaWriter::deleteSubDir( )
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
		oss << "Error deleting bulk rollback data subdirectory " <<
			bulkRollbackSubPath << "; " << boostErrString;
		fLog->logMsg( oss.str(), ERR_FILE_DELETE, MSGLVL_ERROR );
		return ERR_FILE_DELETE;
	}

	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Returns the directory path to be used for storing any backup data files.
//------------------------------------------------------------------------------
std::string RBMetaWriter::getSubDirPath( ) const
{
	std::string bulkRollbackSubPath( fMetaFileName );
	bulkRollbackSubPath += DATA_DIR_SUFFIX;

	return bulkRollbackSubPath;
}

//------------------------------------------------------------------------------
// Backup the contents of the HWM chunk for the specified column OID extent,
// so that the chunk is available for bulk rollback, if needed.
// This operation is only performed for compressed columns.
//------------------------------------------------------------------------------
int RBMetaWriter::backupColumnHWMChunk(
	OID       columnOID,
	uint16_t  dbRoot,
	uint32_t  partition,
	uint16_t  segment,
	HWM       startingHWM)
{
	return backupHWMChunk( true,
		columnOID, dbRoot, partition, segment, startingHWM );
}

//------------------------------------------------------------------------------
// Backup the contents of the HWM chunk for the specified dictionary store OID
// extent, so that the chunk is available for bulk rollback, if needed.
// This operation is only performed for compressed columns.  Once the chunk is
// saved, we remove that OID, partition, and segment
//------------------------------------------------------------------------------
int RBMetaWriter::backupDctnryHWMChunk(
	OID       dctnryOID,
	uint16_t  dbRoot,
	uint32_t  partition,
	uint16_t  segment)
{
	int rc = NO_ERROR;

	if (fRBChunkDctnrySet.size() > 0)
	{
		RBChunkInfo chunkInfo(
			dctnryOID, 0, partition, segment, 0);
		RBChunkInfo chunkInfoFound(0,0,0,0,0);
		bool bFound = false;

		{ // Use scoped lock to perform "find"
			boost::mutex::scoped_lock lock( fRBChunkDctnryMutex );
			if (fLog->isDebug( DEBUG_1 ))
				printDctnryChunkList(chunkInfo, "when searching ");
			RBChunkSet::iterator iter = fRBChunkDctnrySet.find ( chunkInfo );
			if (iter != fRBChunkDctnrySet.end())
			{
				bFound = true;
				chunkInfoFound = *iter;
			}
		}

		if (bFound)
		{
			if (chunkInfoFound.fPartition == partition)
			{
				rc = backupHWMChunk(false,
					dctnryOID, dbRoot, partition, segment, chunkInfoFound.fHwm);
			}
			else
			{
				// How could this happen?  Ended up asking for different
				// partition than expected for the first instance of this
				// OID and segment file.  Perhaps initial blockskipping
				// or something caused us to advance to another segment file
				// without ever changing the expected extent.  At any rate
				// we still fall through and delete our entry because we
				// apparently did not end up changing the chunk referenced
				// by this RBChunkInfo object.
			}

			{ // Use scoped lock to perform "erase"
				boost::mutex::scoped_lock lock( fRBChunkDctnryMutex );
				fRBChunkDctnrySet.erase( chunkInfoFound );
				if (fLog->isDebug( DEBUG_1 ))
					printDctnryChunkList(chunkInfoFound, "after deleting ");
			}
		}
	}

	return rc;
}

//------------------------------------------------------------------------------
// Backup the contents of the HWM chunk for the specified columnOID,dbRoot,etc,
// so that the chunk is available for bulk rollback, if needed.
// This operation is only performed for compressed columns.
//------------------------------------------------------------------------------
int RBMetaWriter::backupHWMChunk(
	bool      bColumnFile, // is this a column (vs dictionary) file
	OID       columnOID,   // OID of column or dictionary store
	uint16_t  dbRoot,      // DB Root for db segment file
	uint32_t  partition,   // partition for db segment file
	uint16_t  segment,     // segment for db segment file
	HWM       startingHWM) // starting HWM for db segment file
{
	std::string fileType("column");
	if (!bColumnFile)
		fileType = "dictionary";

	// Open the applicable database column segment file
	FileOp fileOp;
	std::string segFile;
	FILE* dbFile = fileOp.openFile( columnOID,
		dbRoot,
		partition,
		segment,
		segFile );
	if ( !dbFile )
	{
		std::ostringstream oss;
		oss << "Backup error opening " << fileType <<
			" file for OID-" << columnOID <<
			"; DBRoot-"      << dbRoot    <<
			"; partition-"   << partition <<
			"; segment-"     << segment;
		fLog->logMsg( oss.str(), ERR_FILE_OPEN, MSGLVL_ERROR );

		return ERR_FILE_OPEN;
	}

	// Get the size of the file, so we know where to truncate back to, if needed
	long long fileSizeBytes;
	int rc = fileOp.getFileSize2( dbFile, fileSizeBytes);
	if (rc != NO_ERROR)
	{
		WErrorCodes ec;
		std::ostringstream oss;
		oss << "Backup error getting file size for " << fileType <<
			" OID-"        << columnOID <<
			"; DBRoot-"    << dbRoot    <<
			"; partition-" << partition <<
			"; segment-"   << segment   <<
			"; " << ec.errorString(rc);
		fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

		fileOp.closeFile( dbFile );
		return rc;
	}

	// Read Control header
	char controlHdr[ IDBCompressInterface::HDR_BUF_LEN ];
	rc = fileOp.readFile( dbFile, (unsigned char*)controlHdr,
		IDBCompressInterface::HDR_BUF_LEN );
	if (rc != NO_ERROR)
	{
		WErrorCodes ec;
		std::ostringstream oss;
		oss << "Backup error reading " << fileType <<
			" file control hdr for OID-" << columnOID <<
			"; DBRoot-"          << dbRoot    <<
			"; partition-"       << partition <<
			"; segment-"         << segment   <<
			"; " << ec.errorString(rc);
		fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

		fileOp.closeFile( dbFile );
		return rc;
	}

	IDBCompressInterface compressor;
	int rc1 = compressor.verifyHdr( controlHdr );
	if (rc1 != 0)
	{
		rc = ERR_METADATABKUP_COMP_VERIFY_HDRS;

		WErrorCodes ec;
		std::ostringstream oss;
		oss << "Backup error verifying " << fileType <<
			" file control hdr for OID-" << columnOID <<
			"; DBRoot-"          << dbRoot    <<
			"; partition-"       << partition <<
			"; segment-"         << segment   <<
			"; " << ec.errorString(rc);
		fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
		
		fileOp.closeFile( dbFile );
		return rc;
	}

	// Read Pointer header data
	uint64_t hdrSize    = compressor.getHdrSize(controlHdr);
	uint64_t ptrHdrSize = hdrSize - IDBCompressInterface::HDR_BUF_LEN;
	char* pointerHdr    = new char[ptrHdrSize];
	rc = fileOp.readFile( dbFile, (unsigned char*)pointerHdr, ptrHdrSize );
	if (rc != NO_ERROR)
	{
		WErrorCodes ec;
		std::ostringstream oss;
		oss << "Backup error reading " << fileType <<
			" file pointer hdr for OID-" << columnOID <<
			"; DBRoot-"          << dbRoot    <<
			"; partition-"       << partition <<
			"; segment-"         << segment   <<
			"; " << ec.errorString(rc);
		fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

		fileOp.closeFile( dbFile );
		return rc;
	}

	CompChunkPtrList     chunkPtrs;
	rc = compressor.getPtrList(pointerHdr, ptrHdrSize, chunkPtrs );
	delete[] pointerHdr;
	if (rc != 0)
	{
		std::ostringstream oss;
		oss << "Backup error getting " << fileType <<
			" file hdr for OID-" << columnOID <<
			"; DBRoot-"          << dbRoot    <<
			"; partition-"       << partition <<
			"; segment-"         << segment;
		fLog->logMsg( oss.str(), ERR_METADATABKUP_COMP_PARSE_HDRS,
			MSGLVL_ERROR );

		fileOp.closeFile( dbFile );
		return ERR_METADATABKUP_COMP_PARSE_HDRS;
	}

	// Locate HWM chunk
	unsigned int chunkIndex             = 0;
	unsigned int blockOffsetWithinChunk = 0;
	unsigned char* buffer               = 0;
	uint64_t chunkSize                  = 0;
	compressor.locateBlock(startingHWM, chunkIndex, blockOffsetWithinChunk);

	if (chunkIndex < chunkPtrs.size())
	{
		chunkSize = chunkPtrs[chunkIndex].second;

		// Read the HWM chunk
		rc = fileOp.setFileOffset(dbFile,chunkPtrs[chunkIndex].first,SEEK_SET);
		if (rc != NO_ERROR)
		{
			WErrorCodes ec;
			std::ostringstream oss;
			oss << "Backup error seeking in " << fileType <<
				" file for OID-" << columnOID <<
				"; DBRoot-"      << dbRoot    <<
				"; partition-"   << partition <<
				"; segment-"     << segment   <<
				"; " << ec.errorString(rc);
			fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

			fileOp.closeFile( dbFile );
			return rc;
		}

		buffer = new unsigned char[ chunkPtrs[chunkIndex].second ];
		rc = fileOp.readFile( dbFile, buffer, chunkPtrs[chunkIndex].second );
		if (rc != NO_ERROR)
		{
			WErrorCodes ec;
			std::ostringstream oss;
			oss << "Backup error reading in " << fileType <<
				" file for OID-" << columnOID <<
				"; DBRoot-"      << dbRoot    <<
				"; partition-"   << partition <<
				"; segment-"     << segment   <<
				"; " << ec.errorString(rc);
			fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

			delete []buffer;
			fileOp.closeFile( dbFile );
			return rc;
		}
	}
	else if (startingHWM == 0)
	{
		// Okay to proceed.  Empty file with no chunks.  Save 0 length chunk.
	}
	else
	{
		rc = ERR_METADATABKUP_COMP_CHUNK_NOT_FOUND;

		WErrorCodes ec;
		std::ostringstream oss;
		oss << "Backup error for " << fileType <<
			" file for OID-" << columnOID <<
			"; DBRoot-"      << dbRoot    <<
			"; partition-"   << partition <<
			"; segment-"     << segment   <<
			"; hwm-"         << startingHWM <<
			"; not in hdrPtrs"  <<
			"; " << ec.errorString(rc);
		fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

		fileOp.closeFile( dbFile );
		return rc;
	}

	// Backup the HWM chunk
	std::string errMsg;
	rc = writeHWMChunk(bColumnFile, columnOID, partition, segment,
		buffer, chunkSize, fileSizeBytes, startingHWM, errMsg);
	if (rc != NO_ERROR)
	{
		std::ostringstream oss;
		oss << "Backup error writing backup for " << fileType <<
			" OID-"        << columnOID <<
			"; DBRoot-"    << dbRoot    <<
			"; partition-" << partition <<
			"; segment-"   << segment   <<
			"; "           << errMsg;
		fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );

		delete []buffer;
		fileOp.closeFile( dbFile );
		return rc;
	}
	
	// Close the applicable database column segment file and free memory
	delete []buffer;
	fileOp.closeFile( dbFile );

	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Writes out the specified HWM chunk to disk, in case we need it for bulk
// rollback.  If an error occurs, errMsg will contain the error message.
// This function is careful not to create a corrupt file (should the system
// crash in the middle of writing the file for example).  It's imperative
// that during a failure of any kind, that we not "accidentally" create and
// leave around a corrupt or incomplete HWM backup file that could cause a
// bulk rollback to fail, and eventually corrupt a data base file.
// So this function first creates the HWM backup file to a temp file, and
// after it is successfully created, it is renamed to the final destination.
// If anything goes wrong, we try to delete any files we were creating.
//------------------------------------------------------------------------------
int RBMetaWriter::writeHWMChunk(
	bool                 bColumnFile, // is this a column (vs dictionary) file
	OID                  columnOID,   // OID of column or dictionary store
	uint32_t             partition,   // partition for db segment file
	uint16_t             segment,     // segment for db segment file
	const unsigned char* compressedOutBuf, // compressed chunk to be written
	uint64_t             chunkSize,   // number of bytes in compressedOutBuf
	uint64_t             fileSize,    // size of file in bytes
	HWM                  chunkHWM,    // HWM in the chunk being written
	std::string&         errMsg) const// error msg if error occurs
{
	std::ostringstream ossFile;
	ossFile << "/" << columnOID << ".p" << partition << ".s" << segment;
	std::string fileName = getSubDirPath();
	fileName += ossFile.str();

	std::string fileNameTmp = fileName;
	fileNameTmp += ".tmp";

	//if (fLog->isDebug( DEBUG_1 ))
	{
		std::string fileType("column");
		if (!bColumnFile)
			fileType = "dictionary";

		std::ostringstream oss;
		oss << "Backing up HWM chunk for " << fileType <<
			" OID-"       << columnOID <<
			"; file-"     << fileNameTmp <<
			"; HWM-"      << chunkHWM    <<
			"; bytes-"    << chunkSize   <<
			"; fileSize-" << fileSize;
		fLog->logMsg( oss.str(), MSGLVL_INFO2 );
	}

	FILE* backupFile = fopen( fileNameTmp.c_str(), "w+b" );
	if (!backupFile)
	{
		int errRc = errno;
		WErrorCodes ec;
		std::ostringstream oss;
		std::string eMsg;
		Convertor::mapErrnoToString(errRc, eMsg);
		oss << ec.errorString(ERR_METADATABKUP_COMP_OPEN_BULK_BKUP) <<
			"; " << eMsg;
		errMsg = oss.str();
		return ERR_METADATABKUP_COMP_OPEN_BULK_BKUP;
	}

	uint64_t sizeHdr[2];
	sizeHdr[0] = chunkSize;
	sizeHdr[1] = fileSize;
	size_t itemsWritten = fwrite(sizeHdr,
								sizeof(uint64_t)*2,
								1,
								backupFile);
	if (itemsWritten != 1)
	{
		int errRc = errno;
		WErrorCodes ec;
		std::ostringstream oss;
		std::string eMsg;
		Convertor::mapErrnoToString(errRc, eMsg);
		oss << ec.errorString(ERR_METADATABKUP_COMP_WRITE_BULK_BKUP) <<
			"; " << eMsg;
		errMsg = oss.str();

		fclose( backupFile );
		unlink( fileNameTmp.c_str() );
		return ERR_METADATABKUP_COMP_WRITE_BULK_BKUP;
	}

	if (chunkSize > 0)
	{
		itemsWritten = fwrite(compressedOutBuf,
							chunkSize,
							1,
							backupFile);
		if (itemsWritten != 1)
		{
			int errRc = errno;
			WErrorCodes ec;
			std::ostringstream oss;
			std::string eMsg;
			Convertor::mapErrnoToString(errRc, eMsg);
			oss << ec.errorString(ERR_METADATABKUP_COMP_WRITE_BULK_BKUP) <<
				"; " << eMsg;
			errMsg = oss.str();

			fclose( backupFile );
			unlink( fileNameTmp.c_str() );
			return ERR_METADATABKUP_COMP_WRITE_BULK_BKUP;
		}
	}

	fclose( backupFile );

#ifdef _MSC_VER
	//Windows rename() behaves differently from Linux: it will return an error
	// if the target exists
	//FIXME: The Linux version seems a bit safer, perhaps implement a better
	// Windows port?
	unlink(fileName.c_str());
#endif

	// Rename HWM backup file to final name.
	if ( rename(fileNameTmp.c_str(), fileName.c_str()) )
	{
		int errRc = errno;
		WErrorCodes ec;
		std::ostringstream oss;
		std::string eMsg;
		Convertor::mapErrnoToString(errRc, eMsg);
		oss << ec.errorString(ERR_METADATABKUP_COMP_RENAME) << "; " << eMsg;
		errMsg = oss.str();
		//FIXME: do we really want to unlink BOTH files? (or _any_ files, ftm?)
		unlink( fileNameTmp.c_str() );
		unlink( fileName.c_str() );
		return ERR_METADATABKUP_COMP_RENAME;
	}

	return NO_ERROR;
}

//------------------------------------------------------------------------------
// Prints list of compressed dictionary HWM chunks that we are tracking,
// in order to backup to disk as needed, before we start adding rows to a
// previously existing chunk.
//------------------------------------------------------------------------------
void RBMetaWriter::printDctnryChunkList(
	const RBChunkInfo& rbChk, 
	const char* assocAction)
{
	std::ostringstream oss;
	oss << "Dumping metaDictHWMChunks " << assocAction <<
		rbChk << ":";

	if (fRBChunkDctnrySet.size() > 0)
	{
		RBChunkSet::iterator iter = fRBChunkDctnrySet.begin();
		int k = 1;
		while (iter != fRBChunkDctnrySet.end())
		{
			oss << std::endl;
			oss << '\t' << k << ". " << *iter;

			++k;
			++iter;
		}
	}
	else
	{
		oss << std::endl;
		oss << '\t' << "Empty list";
	}
	fLog->logMsg( oss.str(), MSGLVL_INFO2 );
}

} // end of namespace
