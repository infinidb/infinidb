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
* $Id: we_rbmetawriter.h 4450 2013-01-21 14:13:24Z rdempsey $
*/

/** @file we_rbmetawriter.h
 * Contains class to write HWM-related information used to rollback a
 * cpimport.bin job that abnormally terminated, leaving the db in an
 * inconsistent state.
 * Class was moved from bulk directory for resuse by DML as part of Shared
 * Nothing.
 */

#ifndef WE_RBMETAWRITER_H_
#define WE_RBMETAWRITER_H_

#include <string>
#include <sstream>
#include <set>
#include <map>
#include <vector>
#include <boost/thread/mutex.hpp>

#include "we_type.h"
#include "brmtypes.h"
#include "we_fileop.h"

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

#define DBROOT_BULK_ROLLBACK_SUBDIR "bulkRollback"

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
    OID                fOid;       // dctnry store OID containing relevant chunk
    uint16_t           fDbRoot;    // dbroot, partition, segment of file 
    uint32_t           fPartition; //   containing relevant HWM chunk
    uint16_t           fSegment;   //
    HWM                fHwm;       // HWM block of interest
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
 * Should cpimport.bin terminate abnormally, leaving the db in an inconsistent
 * state, then the information written by this class can be used to perform
 * a bulk rollback, to restore the db to its previous state, prior to the
 * execution of the import job.
 *
 * For SharedEveryThing:
 * Note that column segment files, carry a logical concatenation of extents,
 * so only the HWM of the last logical extent needs to be written to the meta
 * data file, as the information about where the data stops (and needs to
 * be rolled back) for the other segment files can be derived from knowing
 * the last Local HWM.
 *
 * For Shared-Nothing:
 * Column segment files only carry a logical concatenation of extents, within
 * a DBRoot.  So the HWM of the last logical extent in each DBRoot (for a PM)
 * must be written to the meta data file.  The information about where the data
 * stops (and needs to be rolled back) for the other segment files within a
 * DBRoot can be derived from knowing the last Local HWM per DBRoot.
 * One meta data control file is created for each DBRoot, and in fact, the
 * meta file is created under the corresponding DBRoot directory.
 *
 * In the case of dictionary store segment files, each store file is
 * independent of the other segment store files for the same OID.  So for
 * dictionary store segment files, the HWM must be written for each segment
 * file in the last partition for each DBRoot.
 *
 * API Usage:
 * Public functions would typically be called in the following order:
 *   1. Create and initialize Bulk Rollback Metadata object:
 *        RBMetaWriter()
 *        init()
 *   2. Create primary meta data file (see note below);
 *        openMetaFile()
 *        writeColumnMetaData()
 *        writeDictionaryStoreMetaData()
 *        writeDictionaryStoreMetaNoDataMarker()
 *        closeMetaFile()
 *   3. Backup necessary HWM chunks to backup chunk files:   
 *        a. backupColumnHWMChunk()
 *        b. backupDctnryHWMChunk()
 *   4. Delete meta data file and HWM chunk files at end of successful job:
 *        deleteFile()
 *   5. Delete Bulk Rollback Metadata object:
 *        ~RBMetaWriter()
 *
 * Steps 2 and 3a have been replaced with a single call to saveBulkRollback-
 * MetaData().  This is a helper function that takes care of all the calls in
 * steps 2 and 3a.  If an error should occur in saveBulkRollbackMetaData(),
 * closeMetaFile() will automatically be called to close the meta data file.
 *
 * backupDctnryHWMChunk() must be thread-safe as step 3b can be executed in
 * parallel by several threads for different dictionary columns.
 */
//------------------------------------------------------------------------------
class RBMetaWriter
{
public:

    /** @brief RBMetaWriter constructor
     * @param appDesc Description of application that is using RBMetaWriter
     * @param logger Logger to be used for logging messages.
     */
    EXPORT RBMetaWriter (  const std::string& appDesc,
                           Log* logger );

    /** @brief RBMetaWriter destructor
     */
    EXPORT ~RBMetaWriter ( ) { closeMetaFile ( ); }

    /** @brief Initialize this RBMetaWriter object
     * Warning: This function may throw a WeException.
     *
     * @param tableOID OID of the table whose state is to be saved.
     * @param tableName Name of the table associated with tableOID.
     */
    EXPORT void init ( OID  tableOID,
        const std::string& tableName );

    /** @brief Make a backup copy of the specified HWM dictionary store chunk.
     * This operation only applies to compressed columns.  Backup may not be
     * necessary.  Return value indicates whether the specified file needs to
     * be backed up or not.
     * Warning: This function may throw a WeException.
     *
     * This function is thread-safe since concurrent calls could be made by
     * different threads, each for a different dictionary column.
     *
     * @param dctnryOID column OID to be saved
     * @param dbRoot current dbRoot of last local HWM for columnOID
     * @param partition current partition of last local HWM for columnOID
     * @param segment current segment of last local HWM for columnOID
     * @return Indicates whether it is necessary to perform backup
     */
    EXPORT bool backupDctnryHWMChunk (
        OID                dctnryOID,
        uint16_t           dbRoot,
        uint32_t           partition,
        uint16_t           segment );

    /** @brief Delete the rollback meta files associated with this table
     * Warning: This function may throw a WeException.
     */
    EXPORT void deleteFile ( );

    /** @brief Helper function that creates the primary meta data file.
     * Warning: This function may throw a WeException.
     *
     * See class description for more details.
     * @param columns Vector of column information.  The dataFile member in
     *        each columns entry should be filled in with HWM information about
     *        the start extent where rows will be added.  This information is
     *        needed so that the HWM chunk in that extent can be backed up.
     * @param dctnryStoreOids Vector of dictionary store OID associated with
     *        columns vector.  dctnryStoreOid[n] should be 0 if columns[n] is
     *        not a dictionary column.
     * @param dbRootHwmInfoVecCol Vector of EmDbRootHWMInfo_v objects obtained
     *        from multiple calls to DBRM::getDbRootHWMInfo().  There is one
     *        EmDbRootHWMInfo_v entry per column.  Each
     *        EmDbRootHWMInfo_v object carries a vector of DBRoot, HWM, etc
     *        objects representing the current HWM extents for a column's
     *        DBRoots on the local PM.
     */
    EXPORT void saveBulkRollbackMetaData(
        const std::vector<Column>& columns,
        const std::vector<OID>&    dctnryStoreOids,
        const std::vector<BRM::EmDbRootHWMInfo_v>& dbRootHWMInfoVecCol );

    /** @brief Verify that specified version record is for Version 3 */
    static bool verifyVersion3(const char* versionRec);

    /** @brief Verify that specified version record is for Version 4 */
    static bool verifyVersion4(const char* versionRec);

    /** @brief Verify that specified record type is a Column1 record */
    static bool verifyColumn1Rec(const char* recType);

    /** @brief Verify that specified record type is a Column2 record */
    static bool verifyColumn2Rec(const char* recType);

    /** @brief Verify that specified record type is a DStore1 record */
    static bool verifyDStore1Rec(const char* recType);

    /** @brief Verify that specified record type is a DStore2 record */
    static bool verifyDStore2Rec(const char* recType);

private:
    // disable copy constructor and assignment operator
    RBMetaWriter(const RBMetaWriter&);
    RBMetaWriter& operator=(const RBMetaWriter&);

    // Make a backup copy of the specified HWM column chunk.
    // This operation only applies to compressed columns.
    // Warning: This function may throw a WeException.
    //   columnOID column OID to be saved
    //   dbRoot current dbRoot of last local HWM for columnOID
    //   partition current partition of last local HWM for columnOID
    //   segment current segment of last local HWM for columnOID
    //   lastLocalHwm current last local for column OID
    void backupColumnHWMChunk (
        OID                columnOID,
        uint16_t           dbRoot,
        uint32_t           partition,
        uint16_t           segment,
        HWM                lastLocalHwm );

    // This function must be thread-safe since it is called directly by
    // backupDctnryHWMChunk().  Employed by non-hdfs.
    void backupHWMChunk ( 
        bool               bColumnFile,
        OID                columnOID,
        uint16_t           dbRoot,
        uint32_t           partition,
        uint16_t           segment,
        HWM                lastLocalHwm );

    // This function must be thread-safe since it is called directly by
    // backupDctnryHWMFile().   Employed by hdfs.
    void backupHWMFile ( 
        bool               bColumnFile,
        OID                columnOID,
        uint16_t           dbRoot,
        uint32_t           partition,
        uint16_t           segment,
        HWM                lastLocalHwm );

    // Close the current meta data file.
    EXPORT void closeMetaFile ( );

    void createSubDir( const std::string& metaFileName );
    void deleteSubDir( const std::string& metaFileName );
    int  getSubDirPath(const uint16_t dbRoot,
                           std::string& subDirPath ) const;

    // Open a meta data file to save HWM bulk rollback info for tableOID
    // Warning: This function may throw a WeException.
    //   dbRoot is the DBRoot of interest for the applicable table.
    std::string openMetaFile ( uint16_t dbRoot );

    // Rename temporary metadata control file(s) to the permanent name.
    // Filenames are taken from fMetaFileNames.
    // Warning: This function may throw a WeException.
    void renameMetaFile( );

    // Save column meta data to the currently open file.
    // This is the Shared-Nothing version of this function.
    // Warning: This function may throw a WeException.
    //   metaFileName    name of metafile to be written
    //   columnOID       column OID to be saved
    //   dbRoot          current dbRoot of last local HWM for columnOID
    //   partition       current partition of last local HWM for columnOID
    //   segment         current segment of last local HWM for columnOID
    //   lastLocalHwm    current last local for column OID
    //   colType         type of columnOID
    //   colTypeName     type name of columnOID
    //   colWidth        width (in bytes) of columnOID
    //   compressionType compression type
    void writeColumnMetaData (
        const std::string& metaFileName,
        bool               withHWM,
        OID                columnOID,
        uint16_t           dbRoot,
        uint32_t           partition,
        uint16_t           segment,
        HWM                lastLocalHwm,
        execplan::CalpontSystemCatalog::ColDataType colType,
        const std::string& colTypeName,
        int                colWidth,
        int                compressionType );

    // Save dictionary store meta data to the currently open file.
    // This is the Shared-Nothing version of this function.
    //   dictionaryStoreOID dictionary store OID to be saved
    //   dbRoot dbRoot of store file
    //   partition partition of store file
    //   segment segment of store file
    //   localHwm current local HWM for specified partition and seg file
    //   compressionType compression type
    void writeDictionaryStoreMetaData (
        OID                columnOID,
        OID                dictionaryStoreOID,
        uint16_t           dbRoot,
        uint32_t           partition,
        uint16_t           segment,
        HWM                localHwm,
        int                compressionType );

    // For first extent stripe in a partition, this function is used to
    // to log a marker to denote a trailing segment file that does not exist.
    // This is the Shared-Nothing version of this function.
    //   dictionaryStoreOID dictionary store OID to be saved
    //   dbRoot dbRoot of store file
    //   partition partition of store file
    //   segment segment of store file
    //   compressionType compression type
    void writeDictionaryStoreMetaNoDataMarker (
        OID                columnOID,
        OID                dictionaryStoreOID,
        uint16_t           dbRoot,
        uint32_t           partition,
        uint16_t           segment,
        int                compressionType );

    // This function must be thread-safe since it is called indirectly by
    // backupDctnryHWMChunk() (through backupHWMChunk()).
    int writeHWMChunk (
        bool               bColumnFile,
        OID                columnOID,
        uint16_t           dbRoot,
        uint32_t           partition,
        uint16_t           segment,
        const unsigned char* compressedOutBuf,
        uint64_t           chunkSize,
        uint64_t           fileSize,
        HWM                chunkHwm,
        std::string&       errMsg) const;
    void printDctnryChunkList(const RBChunkInfo& rbChk, const char* action);

    IDBDataFile*           fMetaDataFile;     // current meta data file to write
    std::ostringstream     fMetaDataStream;   // adapter for IDBDataFile
    std::map<uint16_t, std::string> fMetaFileNames;//map of dbroots to metafiles
    std::string            fAppDesc;          // description of application user
    Log*                   fLog;              // import log file
    bool                   fCreatedSubDir;    // has subdir path been created
    RBChunkSet             fRBChunkDctnrySet; // Dctnry HWM chunk info
    boost::mutex           fRBChunkDctnryMutex;//Mutex lock for RBChunkSet
    OID                    fTableOID;         // OID of relevant table
    std::string            fTableName;        // Name of relevant table
};

} //end of namespace

#undef EXPORT

#endif // WE_RBMETAWRITER_H_
