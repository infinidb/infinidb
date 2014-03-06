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

/** @file */

#ifndef CONFIRM_HDFS_DBFILE_H
#define CONFIRM_HDFS_DBFILE_H

#include <string>

#include "IDBFileSystem.h"
#include "we_type.h"

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace WriteEngine
{

/** @brief Encapsulates logic to confirm and finalize (or abort) changes
 *  to an HDFS db file.  Class should only be used for HDFS db files.
 *
 *  For the HDFS db files that are modified, the new version of the file
 *  may be written to a file with an alternative suffix (like .tmp file).
 *  The confirmDbFileChange() function confirms that the work is complete,
 *  and that the new temporary file (ex: *.tmp) can replace the original file.
 *  After all the columns in a table have been confirmed (through calls to
 *  confirmDbFileChange()), then endDbFileChange() should be called to finish
 *  committing or aborting all the work.
 */
class ConfirmHdfsDbFile
{
public:
    EXPORT ConfirmHdfsDbFile( );
    EXPORT ~ConfirmHdfsDbFile( );

    /** @brief Confirm changes to the specified db file
     *  @param backUpFileType Backup file type to confirm.  Types:
     *    "rlc" - reallocated chunk file
     *    "tmp" - updated db file
     *  @param filename Name of db file to be confirmed.
     *  @param errMsg (out) Error msg associated with a bad return code
     *  @return Returns NO_ERROR if call is successful
     */
    EXPORT int confirmDbFileChange( const std::string& backUpFileType,
        const std::string& filename,
        std::string& errMsg ) const;

    /** @brief Finalize changes to the specified db file
     *
     *  If success flag is true:
     *    The old version of the db file is deleted.
     *  If success flag is false:
     *    The old version is retained, and any temporary file with pending
     *    changes is deleted.
     *
     *  @param backUpFileType Backup file type to finalize.  Types:
     *    "rlc" - reallocated chunk file
     *    "tmp" - updated db file
     *  @param filename Name of db file to be finalized.
     *  @param success Final success/fail status of db file changes
     *  @param errMsg (out) Error msg associated with a bad return code
     *  @return Returns NO_ERROR if call is successful
     */
    EXPORT int endDbFileChange( const std::string& backUpFileType,
        const std::string& filename,
        bool success,
        std::string& errMsg ) const;

    /** @brief Confirm changes to the db files modified for tableOID
     *
     *  The HWM db file for each DBRoot, as listed in the bulk rollback meta
     *  data file (for the specified tableOID), is confirmed.
     *
     *  @param tableOID Table that has changes to be confirmed
     *  @param errMsg (out) Error msg associated with a bad return code
     *  @return Returns NO_ERROR if call is successful
     */
    EXPORT int confirmDbFileListFromMetaFile( OID tableOID,
        std::string& errMsg );

    /** @brief Finalize changes to the db files modified for tableOID
     *
     *  The HWM db file for each DBRoot, as listed in the bulk rollback meta
     *  data file (for the specified tableOID), is finalized.
     *
     *  If success flag is true:
     *    The old version of the db files are deleted.
     *  If success flag is false:
     *    The old versions are retained, and any temporary files with pending
     *    changes are deleted.
     *
     *  @param tableOID Table that has changes to be confirmed
     *  @param errMsg (out) Error msg associated with a bad return code
     *  @return Returns NO_ERROR if call is successful
     */
    EXPORT int endDbFileListFromMetaFile( OID tableOID,
        bool success,
        std::string& errMsg );

private:
    void openMetaDataFile( OID tableOID,
        uint16_t dbRoot,
        std::istringstream& metaDataStream );

    void confirmDbFiles( std::istringstream& metaDataStream ) const;
    void confirmColumnDbFile( const char* inBuf ) const;
    void confirmDctnryStoreDbFile( const char* inBuf ) const;

    void endDbFiles( std::istringstream& metaDataStream, bool success ) const;
    void endColumnDbFile( const char* inBuf, bool success ) const;
    void endDctnryStoreDbFile( const char* inBuf, bool success ) const;

    idbdatafile::IDBFileSystem& fFs;
    std::string fMetaFileName;
};

} // end of namespace

#undef EXPORT

#endif  // CONFIRM_HDFS_DBFILE_H
