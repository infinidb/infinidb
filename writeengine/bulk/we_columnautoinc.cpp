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
* $Id: we_columnautoinc.cpp 3720 2012-04-04 18:18:49Z rdempsey $
*/

/** @file
 * Implementation of the ColumnAutoInc, ColumnAutoIncJob, and
 * ColumnAutoIncIncremental classes.
 */

#include "we_columnautoinc.h"

#include <exception>
#include <sstream>

#include "we_define.h"
#include "we_bulkload.h"
#include "we_log.h"
#include "we_columninfo.h"
#include "dbrm.h"

#include "calpontsystemcatalog.h"

namespace WriteEngine
{

//------------------------------------------------------------------------------
// ColumnAutoInc constructor.
//------------------------------------------------------------------------------
ColumnAutoInc::ColumnAutoInc( Log* logger ) :
    fLog(logger),
    fAutoIncLastValue(0),
    fColumnOID(0)
{
}

//------------------------------------------------------------------------------
// ColumnAutoInc destructor.
//------------------------------------------------------------------------------
/* virtual */
ColumnAutoInc::~ColumnAutoInc( )
{
}

//------------------------------------------------------------------------------
// Initialize auto-increment column for specified schema and table.
// fullTableName - Schema and table name separated by a period.
// colInfo       - ColumnInfo associated with auto-increment column.
//------------------------------------------------------------------------------
int ColumnAutoInc::init( const std::string& fullTableName,
    ColumnInfo* colInfo )
{
    fMaxIntSat = colInfo->column.fMaxIntSat;
    fTableName = fullTableName;
    fColumnName= colInfo->column.colName;
    fColumnOID = colInfo->column.mapOid;

    std::string::size_type periodIdx = fTableName.find('.');
    if (periodIdx == std::string::npos)
    {
        std::ostringstream oss;
        oss << "Error parsing full table name to get auto-increment value for "
            << fTableName;
        fLog->logMsg( oss.str(), ERR_AUTOINC_TABLE_NAME, MSGLVL_ERROR );
        return ERR_AUTOINC_TABLE_NAME;
    }

    long long nextAuto = 0;
    int rc = getNextValueFromSysCat( nextAuto );
    if (rc != NO_ERROR)
    {
        return rc;
    }

    std::string errMsg;
    rc = BRMWrapper::getInstance()->startAutoIncrementSequence(
        fColumnOID, nextAuto, colInfo->column.width, errMsg );
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "Unable to initialize auto-increment sequence for " <<
            fTableName << "; " << errMsg;
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        return rc;
    }

    std::ostringstream oss2;
    oss2 << "Initializing next auto increment for table-" << fTableName <<
        ", column-"        << fColumnName <<
        "; autoincrement " << nextAuto;
    fLog->logMsg( oss2.str(), MSGLVL_INFO2 );

    initNextAutoInc( nextAuto );

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Initialize last used auto-increment value from the current "next"
// auto-increment value taken from the system catalog.
// Don't need to use fAutoIncMutex in this function as long as we call it from
// the main thread, during preprocessing.  But we go ahead and use the mutex
// for completeness.  Using the mutex should not affect performance, since this
// function is only called once per table. 
//------------------------------------------------------------------------------
void ColumnAutoInc::initNextAutoInc( long long nextValue )
{
    boost::mutex::scoped_lock lock(fAutoIncMutex);
    // nextValue is unusable if < 1; probably means we already reached max value
    if (nextValue < 1)
        fAutoIncLastValue = fMaxIntSat;
    else
        fAutoIncLastValue = nextValue - 1;
}

//------------------------------------------------------------------------------
// Finished working with this auto-increment.  Any remaining steps that are
// necessary to save or commit changes to the auto-increment nextValue, are
// applied here.
//------------------------------------------------------------------------------
int ColumnAutoInc::finish( )
{
    int rc = NO_ERROR;

    // We intentionally use a separate DBRM instance in this function.  We don't
    // use the BRMWrapper singleton.  We do this because the BRM call that is
    // made to issue a lock is a synchronous call that will block till a lock
    // is acquired.  Better to do this in a separate BRM instance, rather than
    // having this call block any other thread using BRM.
    BRM::DBRM dbrm;

    // We grab AI lock in order to access/synchronize DBRM and the system
    // catalog as a single operation, to avoid race condition between apps.
    try {
        dbrm.getAILock( fColumnOID );
    }
    catch (std::exception& ex)
    {
        std::ostringstream oss;
        oss << "Error locking auto-increment nextValue lock for table " <<
            fTableName << "; column " << fColumnName << "; " << ex.what();
        fLog->logMsg( oss.str(), ERR_AUTOINC_GET_LOCK, MSGLVL_ERROR );
        return ERR_AUTOINC_GET_LOCK;
    }

    long long sysCatNextAuto = 0;
    rc = getNextValueFromSysCat( sysCatNextAuto );
    if (rc == NO_ERROR)
    {
        // Update system catalog if my latest AI nextValue is > the current
        // syscat AI nextValue.  -1 denotes an AI column that has maxed out.
        long long myNextValue = getNextAutoIncToSave();
        if ( (sysCatNextAuto != -1) && // do not update if syscat already at max
            ((myNextValue >  sysCatNextAuto) ||
             (myNextValue == -1)) )
        {
            std::ostringstream oss2;
            oss2 << "Updating next auto increment for table-" << fTableName <<
                ", column-"        << fColumnName <<
                "; autoincrement " << myNextValue;
            fLog->logMsg( oss2.str(), MSGLVL_INFO2 );

            rc = BulkLoad::updateNextValue( fColumnOID, myNextValue );
            if (rc != NO_ERROR)
            {
                WErrorCodes ec;
                std::ostringstream oss;
                oss << "Error updating auto-increment nextValue for table " <<
                    fTableName << "; column " << fColumnName << "; rc=" << rc <<
                    "; " << ec.errorString(ERR_AUTOINC_UPDATE);
                fLog->logMsg( oss.str(), ERR_AUTOINC_UPDATE, MSGLVL_ERROR );

                // Don't exit this function yet.  We set return code and fall
                // through to bottom of the function to release the AI lock.
                rc = ERR_AUTOINC_UPDATE;
            }
        }
        else
        {
            std::ostringstream oss2;
            oss2 << "Skip updating next auto increment for table-"<<fTableName<<
                ", column-"        << fColumnName <<
                "; autoincrement " << myNextValue <<
                "; syscat AI already at " << sysCatNextAuto;
            fLog->logMsg( oss2.str(), MSGLVL_INFO2 );
        }
    } // end of rc==NO_ERROR from getNextValueFromSysCat()

    try {
        dbrm.releaseAILock( fColumnOID );
    }
    catch (std::exception& ex)
    {
        // If we have trouble releasing AI lock, we log it, but we don't
        // consider it fatal to the job; so we don't return bad return code.
        std::ostringstream oss;
        oss << "Error releasing auto-increment nextValue lock for table "<<
            fTableName << "; column " << fColumnName << "; " << ex.what();
        fLog->logMsg( oss.str(), ERR_AUTOINC_REL_LOCK, MSGLVL_WARNING );
        //return ERR_AUTOINC_REL_LOCK;
    }

    return rc;
}

//------------------------------------------------------------------------------
// Return "next" auto-increment value, based on last used auto-increment
// value tracked by this ColumnInfo object, that can/should be saved back
// into the system catalog at the end of the job.
//------------------------------------------------------------------------------
long long ColumnAutoInc::getNextAutoIncToSave( )
{
    long long nextValue = -1;

    boost::mutex::scoped_lock lock(fAutoIncMutex);
    // nextValue is returned as -1 if we reached max value
    if (fAutoIncLastValue < fMaxIntSat)
        nextValue = fAutoIncLastValue + 1;

    return nextValue;
}

//------------------------------------------------------------------------------
// Get the current AI nextValue from the system catalog.
//------------------------------------------------------------------------------
int ColumnAutoInc::getNextValueFromSysCat( long long& nextValue )
{
    std::string::size_type periodIdx = fTableName.find('.');

    std::string sName;
    std::string tName;
    sName.assign(fTableName, 0, periodIdx);
    tName.assign(fTableName, periodIdx+1,
        fTableName.length() - (periodIdx+1));
    execplan::CalpontSystemCatalog::TableName tbl(sName,tName);
    long long nextAuto = 0;
    try
    {
        execplan::CalpontSystemCatalog* systemCatPtr =
            execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(
                BULK_SYSCAT_SESSION_ID);
        systemCatPtr->identity(execplan::CalpontSystemCatalog::EC);

        // Handle bad return code or thrown exception from
        // system catalog query.
        nextAuto = systemCatPtr->nextAutoIncrValue( tbl );
        if (nextAuto == 0) {
            throw std::runtime_error(
                "Not an auto-increment column, or column not found");
        }
        else if (nextAuto == -2) {
            throw std::runtime_error(
                "Not able to get current nextValue, table not found");
        }
        else if (nextAuto <   0) {
            throw std::runtime_error(
                "auto-increment max value already reached");
        }

        nextValue = nextAuto;
    }
    catch (std::exception& ex)
    {
        std::ostringstream oss;
        oss << "Unable to get current auto-increment value for " <<
            sName << "." << tName << "; " << ex.what();
        fLog->logMsg( oss.str(), ERR_AUTOINC_INIT1, MSGLVL_ERROR );
        return ERR_AUTOINC_INIT1;
    }
    catch (...)
    {
        std::ostringstream oss;
        oss << "Unable to get current auto-increment value for " <<
            sName << "." << tName << "; unknown exception";
        fLog->logMsg( oss.str(), ERR_AUTOINC_INIT2, MSGLVL_ERROR );
        return ERR_AUTOINC_INIT2;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// ColumnAutoIncJob constructor.
//------------------------------------------------------------------------------
ColumnAutoIncJob::ColumnAutoIncJob( Log* logger ) : ColumnAutoInc(logger)
{
}

//------------------------------------------------------------------------------
// ColumnAutoIncJob destructor.
//------------------------------------------------------------------------------
/* virtual */
ColumnAutoIncJob::~ColumnAutoIncJob( )
{
}

//------------------------------------------------------------------------------
// Reserve specified number of auto-increment numbers.
// Returns starting nextValue associated with the reserved range of numbers.
//------------------------------------------------------------------------------
/* virtual */
int ColumnAutoIncJob::reserveNextRange(
    uint autoIncCount,
    long long& nextValue )
{
    boost::mutex::scoped_lock lock(fAutoIncMutex);
    if ((fMaxIntSat - autoIncCount) < fAutoIncLastValue)
    {
        return ERR_AUTOINC_GEN_EXCEED_MAX;
    }

    nextValue = fAutoIncLastValue + 1;
    fAutoIncLastValue += autoIncCount;

    return NO_ERROR;
}


//------------------------------------------------------------------------------
// ColumnAutoIncIncremental constructor.
//------------------------------------------------------------------------------
ColumnAutoIncIncremental::ColumnAutoIncIncremental( Log* logger ) :
    ColumnAutoInc(logger)
{
}

//------------------------------------------------------------------------------
// ColumnAutoIncIncremental destructor.
//------------------------------------------------------------------------------
/* virtual */
ColumnAutoIncIncremental::~ColumnAutoIncIncremental( )
{
}

//------------------------------------------------------------------------------
// Reserve specified number of auto-increment numbers.
// Returns starting nextValue associated with the reserved range of numbers.
//------------------------------------------------------------------------------
/* virtual */
int ColumnAutoIncIncremental::reserveNextRange(
    uint autoIncCount,
    long long& nextValue )
{
    u_int64_t countArg   = autoIncCount;
    u_int64_t nextValArg = 0;
    std::string errMsg;
    int rc = BRMWrapper::getInstance()->getAutoIncrementRange(
        fColumnOID, countArg, nextValArg, errMsg );
    if (rc != NO_ERROR)
    {
        std::ostringstream oss;
        oss << "Error reserving auto-increment range (" << countArg <<
            " numbers) for table "<<
            fTableName << "; column " << fColumnName << "; " << errMsg;
        if (rc == ERR_AUTOINC_GEN_EXCEED_MAX)
            oss << " Max allowed value is " << fMaxIntSat << ".";
        fLog->logMsg( oss.str(), rc, MSGLVL_ERROR );
        return rc;
    }

    nextValue = nextValArg;
    long long autoIncLastValue = nextValue + autoIncCount - 1;

    // For efficiency we delay the mutex till now, instead of before the call
    // to getAutoIncrementRange().  This means we could theoretically end up
    // processing AI ranges out of order, so we don't arbitrarily
    // update fAutoIncLastValue.  We only update it if the range in question
    // exceeds the current value for fAutoIncLastValue.
    boost::mutex::scoped_lock lock(fAutoIncMutex);
    if (autoIncLastValue > fAutoIncLastValue)
        fAutoIncLastValue = autoIncLastValue;

    return NO_ERROR;
}

} // end of namespace
