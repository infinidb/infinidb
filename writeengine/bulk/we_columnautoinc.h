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
* $Id: we_columnautoinc.h 4450 2013-01-21 14:13:24Z rdempsey $
*/

/** @file we_columnautoinc.h
 * Contains classes to manage the "next value" for an auto-increment column.
 */

#ifndef WE_COLUMNAUTOINC_H_
#define WE_COLUMNAUTOINC_H_

#include <string>
#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>

#include "we_type.h"

namespace WriteEngine
{
    struct ColumnInfo;
    class Log;

//------------------------------------------------------------------------------
/** @brief Abstract base class used for derived auto-increment classes.
 */
//------------------------------------------------------------------------------
class ColumnAutoInc
{
public:
    /** @brief ColumnAutoInc constructor.
     * @param logger Object use in logging.
     */
    explicit ColumnAutoInc(Log* logger);

    /** @brief ColumnAutoInc destructor.
     */
    virtual ~ColumnAutoInc();

    /** @brief Initialize auto-increment next value.
     * @param fullTableName Name of table (schema.table) containing this column
     * @param colInfo The auto-increment column.
     */
    int init( const std::string& fullTableName,
                        ColumnInfo* colInfo );

    /** @brief Reserve next range of auto-increment numbers.
     * @param autoIncCount Number of auto-increment numbers to reserve
     * @param nextValue Starting number of reserved range
     */
    virtual int reserveNextRange(uint32_t autoIncCount,
                        uint64_t& nextValue) = 0;

    /** @brief Finished with auto-incrementing; perform any applicable updates.
     */
    int finish( );

protected:
    void initNextAutoInc(uint64_t nextValue);
    uint64_t getNextAutoIncToSave( );
    int getNextValueFromSysCat(uint64_t& nextValue);

    Log*        fLog;           // import log file
    boost::mutex fAutoIncMutex; // Mutex to manage fAutoIncLastValue
    uint64_t    fAutoIncLastValue;// Tracks latest autoincrement value used
    uint64_t    fMaxIntSat;     // Maximum saturation value
    std::string fTableName;     // Full table name (schema.table) for AI column
    std::string fColumnName;    // Name of auto-increment column
    OID         fColumnOID;     // Column OID
};

//------------------------------------------------------------------------------
/** @brief Auto-increment implementation that reserves auto-increment numbers
 *  once for the entire job.  
 *
 *  Assumes a lock is applied to the table throughout the life of the job.
 *  This allows the auto-increment next value to be managed through 2 single
 *  calls; a call at the start of the job to acquire the starting value, and a
 *  call at the end of the job to save the latest value used.  This implementa-
 *  tion of ColumnAutoInc can only be used for shared-everything.
 */
//------------------------------------------------------------------------------
class ColumnAutoIncJob : public ColumnAutoInc
{
public:
    explicit ColumnAutoIncJob(Log* logger);
    virtual ~ColumnAutoIncJob();

    virtual int reserveNextRange(uint32_t autoIncCount,
                        uint64_t& nextValue);
};

//------------------------------------------------------------------------------
/** @brief Auto-increment implementation that reserves auto-increment numbers
 *  incrementally from the source (system catalog or BRM).
 *
 *  This implementation is necessary for shared-nothing where parallel bulk
 *  loads are allowed, thus leaving the door open for potential race conditions
 *  if ColumnAutoIncJob is used.
 */
//------------------------------------------------------------------------------
class ColumnAutoIncIncremental : public ColumnAutoInc
{
public:
    explicit ColumnAutoIncIncremental(Log* logger);
    virtual ~ColumnAutoIncIncremental();

    virtual int reserveNextRange(uint32_t autoIncCount,
                        uint64_t& nextValue);
};

} //end of namespace

#endif // WE_COLUMNAUTOINC_H_
