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
* $Id: we_bulkrollbackmgr.h 4726 2013-08-07 03:38:36Z bwilkinson $
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
#include <set>
#include <string>
#include <sstream>
#include <vector>

#include "we_type.h"
#include "messagelog.h"
#include "messageobj.h"

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace WriteEngine
{
    class Log;
    class BulkRollbackFile;

//------------------------------------------------------------------------------
/** @brief Class to clear a database table lock, and rolls back extents
 *  based on HWM meta data saved by a bulk load.
 */
//------------------------------------------------------------------------------
class BulkRollbackMgr
{
    enum BulkRollbackVersion
    {
        BULK_RB_VERSION_OTHER,
        BULK_RB_VERSION3 = 3,
        BULK_RB_VERSION4 = 4
    };

public:
    /**
     * @brief BulkRollbackMgr constructor
     * @param tableOID table to be rolled back.
     * @param lockID Table lock id of the table to be rolled back.
     *        Currently used for logging only.
     * @param tableName name of table associated with tableOID.
     *        Currently used for logging only.
     * @param applName application that is driving this bulk rollback.
     *        Currently used for logging only.
     */
    EXPORT BulkRollbackMgr(OID tableOID,
                           uint64_t lockID,
                           const std::string& tableName,
                           const std::string& applName,
                           Log* logger=0);

    /**
     * @brief BulkRollbackMgr destructor
     */
    EXPORT ~BulkRollbackMgr( ) { closeMetaDataFile ( ); }

    /**
     * @brief Clear table lock and rollback extents for fTableOID
     * @param keepMetaFile controls whether the meta data file is deleted
     * @return NO_ERROR upon success
     */
    EXPORT int rollback (  bool keepMetaFile );

    /**
     * @brief Accessor to any error msg related to a bad return code.
     * @return error message if rollback rejected or failed.
     */
    const std::string& getErrorMsg( ) const { return fErrorMsg; }

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
    /**
     * @brief Standalone utility that can be used to delete the bulk rollback
     * meta data files.  Caution: this function can throw an exception.
     * @param tableOID Bulk rollback files for this table are to be deleted
     */
    EXPORT static void deleteMetaFile( OID tableOID );

    /*
     * @brief Get list of segment file numbers found in dirName directory
     * @param dirName Directory path to be searched
     * @param bIncludeAlternateSegFileNames Include *.orig and *.tmp in search
     * @param segList List of segment files found in dirName
     * @param errMsg Error msg if return code is not NO_ERROR
     */
    EXPORT static int getSegFileList( const std::string& dirName,
                                bool bIncludeAlternateSegFileNames,
                                std::vector<uint32_t>& segList,
                                std::string& errMsg );

private:
    // Declare but don't define copy constructor and assignment operator
    BulkRollbackMgr(const BulkRollbackMgr& rhs);
    BulkRollbackMgr& operator=(const BulkRollbackMgr& rhs);

    // Structure used to store info for the list of dictionary store
    // segment files in the last partition.
    struct RollbackData
    {
        uint32_t    fDbRoot;
        uint32_t    fPartNum;
        uint32_t    fSegNum;
        HWM          fHwm;
        bool         fWithHwm;
    };

    void createFileDeletionEntry( OID     columnOID,
                                bool      fileTypeFlag,
                                uint32_t dbRoot,
                                uint32_t partNum,
                                uint32_t segNum,
                                const std::string& segFileName );
    void deleteColumn1Extents ( const char* inBuf ); // delete col extents
    void deleteColumn1ExtentsV3(const char* inBuf );
    void deleteColumn1ExtentsV4(const char* inBuf );
    void deleteColumn2Extents ( const char* inBuf ); // delete col extents
    void deleteColumn2ExtentsV3(const char* inBuf );
    void deleteColumn2ExtentsV4(const char* inBuf );
    void deleteDbFiles        ( ); // delete DB files waiting to be deleted
    void deleteDctnryExtents  ( ); // delete dictionary store extents
    void deleteDctnryExtentsV3( );
    void deleteDctnryExtentsV4( );
    void deleteExtents        ( std::istringstream& metaDataStream );
                                   // function that drives extent deletion
    void readMetaDataRecDctnry(const char* inBuf );//read meta-data dct rec

    void deleteSubDir         ( const std::string& metaFileName ); // delete
                                   // subdirectory used for backup chunks
    EXPORT void closeMetaDataFile    ( ); // close a metafile
    void deleteMetaDataFiles  ( ); // delete metafiles
    int  metaDataFileExists   ( bool& exists ); // does meta-data file exists
    BulkRollbackFile* makeFileRestorer(int compressionType);
    bool openMetaDataFile     ( uint16_t dbRoot,    //  open a metadata file
                                std::istringstream& metaDataStream );
    void validateAllMetaFilesExist(const std::vector<uint16_t>& dbRoots) const;

    // Data members
    OID           fTableOID;    // table to be rolled back
    uint64_t     fLockID;      // unique lock ID associated with table lock
    std::string   fTableName;   // name of table associated with fTableOID
    uint32_t     fProcessId;   // pid associated with current table lock
    std::string   fProcessName; // processName associated with fProcessId
    IDBDataFile*  fMetaFile;    // current meta data file we are reading
    std::string   fMetaFileName;// name of current meta data file
    std::vector<std::string> fMetaFileNames; // all relevant meta data files
    std::string   fErrorMsg;
    unsigned char fDctnryHdr[DCTNRY_HEADER_SIZE]; // empty dctnry store blk

    // Dictionary store extents for an OID are read in and managed as a
    // group.  The following data members are used to collect this info.
    OID           fPendingDctnryStoreOID;// Dctnry OID of pending dctnry extents
    uint32_t     fPendingDctnryStoreDbRoot; // DbRoot of pending dctnry extents
    int           fPendingDctnryStoreCompressionType; // Dctnry compression type
    std::vector<RollbackData> fPendingDctnryExtents;
    std::set<OID> fAllColDctOIDs;   // List of all affected col and dctnry OIDS

    // List of DB Files to be deleted.  Files are deleted in reverse order.
    std::vector<File>         fPendingFilesToDelete;

    logging::MessageLog fSysLogger; // Used for syslogging
    bool          fDebugConsole;    // control debug logging to console
    Log*          fLog;             // optional logger object
    std::string   fApplName;        // application initiating the bulk rollback
    int           fVersion;         // version of meta data file being read
};

} //end of namespace

#undef EXPORT

#endif // WE_BULKROLLBACKMGR_H_
