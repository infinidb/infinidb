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
* $Id$
*/

/** @file we_rbmetawriter.h
 * Contains class to write HWM-related information used to rollback a cpimport
 * job that abnormally terminated, leaving the db in an inconsistent state.
 */

#ifndef WE_RBMETAWRITER_H_
#define WE_RBMETAWRITER_H_

#include <string>
#include <fstream>
#include <set>
#include <boost/thread/mutex.hpp>

#include "we_type.h"

namespace WriteEngine
{
	class Log;

//------------------------------------------------------------------------------
/** @brief Class used to store Dictionary store file information used in backing
 * up HWM chunks "as needed".
 */
//------------------------------------------------------------------------------
struct RBChunkInfo
{
	OID                fOid;
	uint16_t           fDbRoot;
	uint32_t           fPartition;
	uint16_t           fSegment;
	HWM                fHwm;
	RBChunkInfo(OID oid, uint16_t dbRoot, uint32_t partition,
		uint16_t segment, HWM hwm ) :
		fOid(oid), fDbRoot(dbRoot), fPartition(partition),
		fSegment(segment), fHwm(hwm) { }
};

class RBChunkInfoCompare
{
public:
	bool operator()(const RBChunkInfo& lhs, const RBChunkInfo& rhs) const;
};

typedef std::set< RBChunkInfo, RBChunkInfoCompare > RBChunkSet;

//------------------------------------------------------------------------------
/** @brief Class to write HWM-related information to support bulk rollbacks.
 * 
 * Should cpimport terminate abnormally, leaving the db in an inconsistent
 * state, then the information written by this class can be used to perform
 * a bulk rollback, to restore the db to its previous state, prior to the
 * execution of the import job.
 *
 * Note that column segment files, carry a logical concatenation of extents,
 * so only the HWM of the last logical extent needs to be written to the meta
 * data file, as the information about where the data stops (and needs to
 * be rolled back) for the other segment files can be derived from knowing
 * the last Local HWM.
 *
 * In the case of dictionary store segment files, this is not the case,
 * since each store file is independent of the other segment store files for
 * the same OID.  So for dictionary store segment files, the HWM must be
 * written for each segment file in the last partition.
 */
//------------------------------------------------------------------------------
class RBMetaWriter
{
public:

	/** @brief RBMetaWriter constructor
	 * @param logger Logger to be used for logging messages.
	 */
	explicit RBMetaWriter ( Log* logger );

	/** @brief Initialize this RBMetaWriter object
	 * @param tableOID OID of the table whose state is to be saved.
	 * @param tableName Name of the table associated with tableOID.
	 */
	int init ( OID         tableOID,
		const std::string& tableName );

	/** @brief Make a backup copy of the specified HWM column chunk.
	 * This operation only applies to compressed columns.
	 * @param columnOID column OID to be saved
	 * @param dbRoot current dbRoot of last local HWM for columnOID
	 * @param partition current partition of last local HWM for columnOID
	 * @param segment current segment of last local HWM for columnOID
	 * @param lastLocalHwm current last local for column OID
	 */
	 int backupColumnHWMChunk (
		OID                columnOID,
		uint16_t           dbRoot,
		uint32_t           partition,
		uint16_t           segment,
		HWM                lastLocalHwm );

	/** @brief Make a backup copy of the specified HWM dictionary store chunk.
	 * This operation only applies to compressed columns.
	 * @param dctnryOID column OID to be saved
	 * @param dbRoot current dbRoot of last local HWM for columnOID
	 * @param partition current partition of last local HWM for columnOID
	 * @param segment current segment of last local HWM for columnOID
	 */
	int backupDctnryHWMChunk (
		OID                dctnryOID,
		uint16_t           dbRoot,
		uint32_t           partition,
		uint16_t           segment );

	/** @brief Close the current meta data file.
	 * @param saveFile Is the file to be saved for later use.
	 */
	int closeFile ( bool saveFile );

	/** @brief Delete the rollback meta file associated with this table
	 */
	void deleteFile ( );

	/** @brief Open a meta data file to save HWM bulk rollback info for tableOID
	 */
	int  openFile ( );

	/** @brief Save column meta data to the currently open file.
	 * @param columnOID column OID to be saved
	 * @param dbRoot current dbRoot of last local HWM for columnOID
	 * @param partition current partition of last local HWM for columnOID
	 * @param segment current segment of last local HWM for columnOID
	 * @param lastLocalHwm current last local for column OID
	 * @param colType type of columnOID
	 * @param colTypeName type name of columnOID
	 * @param colWidth width (in bytes) of columnOID
	 * @param compressionType compression type
	 */
	int writeColumnMetaData (
		OID                columnOID,
		uint16_t           dbRoot,
		uint32_t           partition,
		uint16_t           segment,
		HWM                lastLocalHwm,
		ColDataType        colType,
		const std::string& colTypeName,
		int                colWidth,
		int                compressionType );

	/** @brief Save dictionary store meta data to the currently open file.
	 * @param dictionaryStoreOID dictionary store OID to be saved
	 * @param dbRoot dbRoot of store file
	 * @param partition partition of store file
	 * @param segment segment of store file
	 * @param localHwm current local HWM for specified partition and seg file
	 * @param compressionType compression type
	 */
	void writeDictionaryStoreMetaData (
		OID                dictionaryStoreOID,
		uint16_t           dbRoot,
		uint32_t           partition,
		uint16_t           segment,
		HWM                localHwm,
		int                compressionType );

	/** @brief For first extent stripe in a partition, this function is used to
	 * to log a marker to denote a trailing segment file that does not exist.
	 * @param dictionaryStoreOID dictionary store OID to be saved
	 * @param dbRoot dbRoot of store file
	 * @param partition partition of store file
	 * @param segment segment of store file
	 * @param compressionType compression type
	 */
	void writeDictionaryStoreMetaNoDataMarker (
		OID                dictionaryStoreOID,
		uint16_t           dbRoot,
		uint32_t           partition,
		uint16_t           segment,
		int                compressionType );

	/** @brief Get the subdirectory path for any back up data files.
	 * @return the directory path for any backup data files.
	 */
	std::string getSubDirPath() const;

private:
	int backupHWMChunk ( 
		bool               bColumnFile,
		OID                columnOID,
		uint16_t           dbRoot,
		uint32_t           partition,
		uint16_t           segment,
		HWM                lastLocalHwm );
	int createSubDir();
	int deleteSubDir();
	int writeHWMChunk (
		bool               bColumnFile,
		OID                columnOID,
		uint32_t           partition,
		uint16_t           segment,
		const unsigned char* compressedOutBuf,
		uint64_t           chunkSize,
		uint64_t           fileSize,
		HWM                chunkHwm,
		std::string&       errMsg) const;
	void printDctnryChunkList(const RBChunkInfo& rbChk, const char* action);

	std::ofstream          fMetaDataFile;     // meta data file we are writing
	std::string            fTmpMetaFileName;  // name of tmp meta data file used
                                              //   during construction phase
	std::string            fMetaFileName;     // name of meta file that's saved
	Log*                   fLog;              // import log file
	bool                   fCreatedSubDir;    // has subdir path been created
	RBChunkSet             fRBChunkDctnrySet; // Dctnry HWM chunk info
	boost::mutex           fRBChunkDctnryMutex;//Mutex lock for RBChunkSet
	OID                    fTableOID;         // OID of relevant table
	std::string            fTableName;        // Name of relevant table
};

} //end of namespace

#endif // WE_RBMETAWRITER_H_
