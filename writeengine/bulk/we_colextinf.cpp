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

/*******************************************************************************
 * $Id: we_colextinf.cpp 4495 2013-01-31 15:24:26Z dcathey $
 *
 ******************************************************************************/

/** @file
 * Contains class to track column information per extent.
 * For ex: this is where we track the min/max values per extent for a column.
 */

#include "we_colextinf.h"
#include "dataconvert.h"

#include <iostream>
#include <sstream>

#include "we_define.h"
#include "we_brm.h"
#include "we_log.h"
#include "we_brmreporter.h"
#include "we_convertor.h"

namespace
{
    typedef std::tr1::unordered_map<
        WriteEngine::RID,WriteEngine::ColExtInfEntry,WriteEngine::uint64Hasher>
        RowExtMap;
}

namespace WriteEngine
{

//------------------------------------------------------------------------------
// Add an entry for the pre-existing extent that we start loading data into at
// the start of a bulk load.  In this case we know the LBID, but have no min/
// max values to start with when adding this first entry to our collection.
//------------------------------------------------------------------------------
// @bug 4806: Added bIsNewExtent; Set CP min/max for very first extent on a PM
void ColExtInf::addFirstEntry( RID         lastInputRow,
                               BRM::LBID_t lbid,
                               bool        bIsNewExtent )
{
    boost::mutex::scoped_lock lock(fMapMutex);

    ColExtInfEntry entry( lbid, bIsNewExtent );
    fMap[ lastInputRow ] = entry;
}

//------------------------------------------------------------------------------
// Add or update an entry for a new extent that we are adding.  In this case
// we have completed a Read buffer, and thus have min/max values, but we may
// not have allocated the extent from the ExtentMap yet (till the 1st output
// buffer is flushed), so we will not have an LBID for the 1st buffer for this
// extent.
//------------------------------------------------------------------------------
void ColExtInf::addOrUpdateEntry( RID     lastInputRow,
                                  int64_t minVal,
                                  int64_t maxVal,
                                  ColDataType colDataType )
{
    boost::mutex::scoped_lock lock(fMapMutex);

    RowExtMap::iterator iter = fMap.find( lastInputRow );
    if (iter == fMap.end()) // Add entry
    {
        ColExtInfEntry entry( minVal, maxVal );
        fMap[ lastInputRow ] = entry;

        fPendingExtentRows.insert( lastInputRow );
    }
    else                    // Update entry
    {
        // If all rows had null value for this column, then minVal will be
        // MAX_INT and maxVal will be MIN_INT (see getCPInfoForBRM()).

        if (iter->second.fMinVal == LLONG_MIN) // init the range
        {
            iter->second.fMinVal = minVal;
            iter->second.fMaxVal = maxVal;
        }
        else                // Update the range
        {
            if (isUnsigned(colDataType))
            {
                if (static_cast<uint64_t>(minVal) 
                    < static_cast<uint64_t>(iter->second.fMinVal))
                    iter->second.fMinVal = minVal;
                if (static_cast<uint64_t>(maxVal)
                    > static_cast<uint64_t>(iter->second.fMaxVal))
                    iter->second.fMaxVal = maxVal;
            }
            else
            {
                if (minVal < iter->second.fMinVal)
                    iter->second.fMinVal = minVal;
                if (maxVal > iter->second.fMaxVal)
                    iter->second.fMaxVal = maxVal;
            }
        }
    }
}

//------------------------------------------------------------------------------
// After flushing an output buffer and allocating it's extent, this function is
// called to save the starting LBID back into the corresponding extent entry.
//------------------------------------------------------------------------------
int ColExtInf::updateEntryLbid( BRM::LBID_t startLbid )
{
    boost::mutex::scoped_lock lock(fMapMutex);

    // fPendingExtentRows is a Set carrying a sorted list of the last Row
    // number in each extent.  We should be allocating/assigning LBIDs in
    // row order, so we get the "first" object in fPendingExtentRows, and
    // that should be the extent corresponding to the LBID we just got.
    std::set<RID>::iterator iterPendingExt = fPendingExtentRows.begin();
    if (iterPendingExt != fPendingExtentRows.end())
    {
        RowExtMap::iterator iter = fMap.find( *iterPendingExt );
        if (iter != fMap.end())
        {
            iter->second.fLbid = startLbid;
        }
        else
        {
            return ERR_BULK_MISSING_EXTENT_ENTRY;
        }

        fPendingExtentRows.erase( iterPendingExt );
    }
    else
    {
        return ERR_BULK_MISSING_EXTENT_ROW;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Get updated Casual Partition (CP) information for BRM for this column at EOJ.
//------------------------------------------------------------------------------
void ColExtInf::getCPInfoForBRM( JobColumn column, BRMReporter& brmReporter )
{
    bool bIsChar = ((column.weType  == WriteEngine::WR_CHAR) &&
         (column.colType != COL_TYPE_DICT));

    boost::mutex::scoped_lock lock(fMapMutex);

    RowExtMap::const_iterator iter = fMap.begin();
    while (iter != fMap.end())
    {
        // If/when we support NULL values, we could have an extent with initial
        // value of min=MAX_BIGINT and max=MIN_BIGINT (see
        // BulkLoadBuffer::parseCol()).  If this occurs, (min>max), we still
        // send min/max to BRM so that the isValid flag can be set to CP_VALID
        // if applicable (indicating an extent with no non-NULL values).
        int64_t minVal = iter->second.fMinVal;
        int64_t maxVal = iter->second.fMaxVal;
        if ( bIsChar )
        {
            // If we have added 1 or more rows, then we should have a valid
            // range in our RowExtMap object, in which case...
            // We swap/restore byte order before sending min/max string to BRM;
            // else we leave fMinVal & fMaxVal set to LLONG_MIN and send as-is,
            // to let BRM know we added no rows.
            if ((iter->second.fMinVal != iter->second.fMaxVal) ||
                (iter->second.fMinVal != LLONG_MIN))
            {
                minVal = static_cast<int64_t>( uint64ToStr(
                    static_cast<uint64_t>(iter->second.fMinVal) ) );
                maxVal = static_cast<int64_t>( uint64ToStr(
                    static_cast<uint64_t>(iter->second.fMaxVal) ) );
            }
        }

        // Log for now; may control with debug flag later
        //if (fLog->isDebug( DEBUG_1 ))
        {
            std::ostringstream oss;
            oss << "Saving CP  update for OID-" << fColOid <<
                   "; lbid-"   << iter->second.fLbid <<
                   "; type-"   << bIsChar            <<
                   "; isNew-"  << iter->second.fNewExtent;
            if (bIsChar)
            {
                char minValStr[sizeof(int64_t) + 1];
                char maxValStr[sizeof(int64_t) + 1];
                memcpy(minValStr, &minVal, sizeof(int64_t));
                memcpy(maxValStr, &maxVal, sizeof(int64_t));
                minValStr[sizeof(int64_t)] = '\0';
                maxValStr[sizeof(int64_t)] = '\0';
                oss << "; minVal: " << minVal << "; (" << minValStr << ")"
                    << "; maxVal: " << maxVal << "; (" << maxValStr << ")";
            }
            else if (isUnsigned(column.dataType))
            {
                oss << "; min: "    << static_cast<uint64_t>(minVal)  <<
                       "; max: "    << static_cast<uint64_t>(maxVal);
            }
            else
            {
                oss << "; min: "    << minVal <<
                       "; max: "    << maxVal;
            }

            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }

        BRM::CPInfoMerge cpInfoMerge;
        cpInfoMerge.startLbid = iter->second.fLbid;
        cpInfoMerge.max       = maxVal;
        cpInfoMerge.min       = minVal;
        cpInfoMerge.seqNum    = -1;     // Not used by mergeExtentsMaxMin
        cpInfoMerge.type      = column.dataType;
        cpInfoMerge.newExtent = iter->second.fNewExtent;
        brmReporter.addToCPInfo( cpInfoMerge );

        ++iter;
    }
    fMap.clear(); // don't need map anymore, so release memory
}

//------------------------------------------------------------------------------
// Print contents of this object to the log file.
//------------------------------------------------------------------------------
void ColExtInf::print( const JobColumn& column )
{
    boost::mutex::scoped_lock lock(fMapMutex);
    bool bIsChar = ((column.weType  == WriteEngine::WR_CHAR) &&
                    (column.colType != COL_TYPE_DICT));
    std::ostringstream oss;
    oss << "ColExtInf Map for OID: " << fColOid;
    RowExtMap::const_iterator iter = fMap.begin();
    while (iter != fMap.end())
    {
        oss << std::endl <<
            "  RowKey-" << iter->first           <<
            "; lbid-"   << iter->second.fLbid;
        if (iter->second.fLbid == (BRM::LBID_t)INVALID_LBID)
            oss << " (unset)";
        oss << "; newExt-" << iter->second.fNewExtent;
        if ( bIsChar )
        {
            // Swap/restore byte order before printing character string
            int64_t minVal = static_cast<int64_t>( uint64ToStr(
                static_cast<uint64_t>(iter->second.fMinVal) ) );
            int64_t maxVal = static_cast<int64_t>( uint64ToStr(
                static_cast<uint64_t>(iter->second.fMaxVal) ) );
            char minValStr[sizeof(int64_t) + 1];
            char maxValStr[sizeof(int64_t) + 1];
            memcpy(minValStr, &minVal, sizeof(int64_t));
            memcpy(maxValStr, &maxVal, sizeof(int64_t));
            minValStr[sizeof(int64_t)] = '\0';
            maxValStr[sizeof(int64_t)] = '\0';
            oss << "; minVal: " << minVal << "; (" << minValStr << ")"
                << "; maxVal: " << maxVal << "; (" << maxValStr << ")";
        }
        else if (isUnsigned(column.dataType))
        {
            oss << "; min: "    << static_cast<uint64_t>(iter->second.fMinVal)  <<
                   "; max: "    << static_cast<uint64_t>(iter->second.fMaxVal);
        }
        else
        {
            oss << "; min: "    << iter->second.fMinVal  <<
                   "; max: "    << iter->second.fMaxVal;
        }
        ++iter;
    }

    oss << std::endl << "  ColExtInf Rows/Extents waiting LBIDs: ";
    std::set<RID>::const_iterator iterPendingExt=fPendingExtentRows.begin();
    while (iterPendingExt != fPendingExtentRows.end())
    {
        oss << *iterPendingExt << ", ";
        ++iterPendingExt;
    }

    fLog->logMsg( oss.str(), MSGLVL_INFO2 );
}

} //end of namespace
