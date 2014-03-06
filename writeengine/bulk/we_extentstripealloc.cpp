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
 * $Id: we_extentstripealloc.cpp 4450 2013-01-21 14:13:24Z rdempsey $
 *
 ******************************************************************************/

/** @file
 * Contains class to allocate a "stripe" of extents for all columns across a tbl
 */

#include "we_extentstripealloc.h"

#include <iostream>
#include <sstream>

#include "we_define.h"
#include "we_log.h"
#include "we_brm.h"

namespace
{
    typedef std::tr1::unordered_multimap<WriteEngine::OID,
                                WriteEngine::AllocExtEntry,
                                WriteEngine::AllocExtHasher> AllocExtMap;
    typedef AllocExtMap::iterator       AllocExtMapIter;
    typedef AllocExtMap::const_iterator ConstAllocExtMapIter;
}

namespace WriteEngine
{

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ExtentStripeAlloc::ExtentStripeAlloc( OID tableOID,
    Log* logger ) : fTableOID(tableOID), fLog(logger), fStripeCount(0)
{
}

//------------------------------------------------------------------------------
// Destructor
// Note: fMap will automatically get cleared by unordered_map destructor,
//       so no need to explicitly call clear() in "this" destructor.
//------------------------------------------------------------------------------
ExtentStripeAlloc::~ExtentStripeAlloc( )
{
}

//------------------------------------------------------------------------------
// Add a column to be associated with the "stripe" allocations for "this"
// ExtentStripeAlloc object.
//------------------------------------------------------------------------------
void ExtentStripeAlloc::addColumn( OID colOID, int colWidth )
{
    boost::mutex::scoped_lock lock(fMapMutex);

    fColOIDs.push_back  ( colOID   );
    fColWidths.push_back( colWidth );
}

//------------------------------------------------------------------------------
// Allocate a "stripe" of column extents for the relevant table associated
// with the specified column OID, and at the specified DBRoot.  The partition
// number, segment number, etc associated with the allocated extent are
// returned in the output arguments.
//
// Note that a multimap is used for the internal extent collection, with the 
// column OID as the key.  We would typically expect that we would not have
// more than 1 extent entry for a given column OID in our map at the same
// time.  If this were always true, then a map (and not a multimap) would
// suffice.
// However, I suppose that if the read buffer were large enough, and the
// column widths small enough, then a parsing thread for a read buffer could
// end up asking for a "stripe" of extents "before" all the allocated extents
// for the previous read buffer have been used.  In this case, we may end
// up needing to store more than 1 column extent for the same column, in
// our internal collection.  Thus a multimap is used.  To handle this case,
// we not only search the internal map for the specified column OID, but we
// also:
// 1. make sure we have an extent for the requested DBRoot
// 2. select the lowest allocated stripe, if there should be more than
//    one column extent with the same DBRoot.
//------------------------------------------------------------------------------
int ExtentStripeAlloc::allocateExtent( OID oid,
    uint16_t     dbRoot,
    uint32_t&    partNum, // used as input for empty DBRoot, else output only
    uint16_t&    segNum,
    BRM::LBID_t& startLbid,
    int&         allocSize,
    HWM&         hwm,
    std::string& errMsg )
{
    int retStatus = NO_ERROR;
    bool bFound   = false;
    AllocExtMapIter extentEntryIter;
    errMsg.clear();
    
    std::pair<AllocExtMapIter,AllocExtMapIter> iters;

    boost::mutex::scoped_lock lock(fMapMutex);

    // Search for an extent matching the requested OID and DBRoot.
    // We also filter by selecting the lowest stripe number.  See
    // function description that precedes this function for more detail.
    iters = fMap.equal_range( oid );
    if (iters.first != iters.second)
    {
        for (AllocExtMapIter it=iters.first; it!=iters.second; ++it)
        {
            if (it->second.fDbRoot == dbRoot)
            {
                if ((!bFound) ||
                    (it->second.fStripeKey <
                     extentEntryIter->second.fStripeKey))
                {
                    extentEntryIter = it;
                }
                bFound = true;
            }
        }
    }

    // Return selected extent
    if (bFound)
    {
        partNum   = extentEntryIter->second.fPartNum;
        segNum    = extentEntryIter->second.fSegNum;
        startLbid = extentEntryIter->second.fStartLbid;
        allocSize = extentEntryIter->second.fAllocSize;
        hwm       = extentEntryIter->second.fHwm;
        errMsg    = extentEntryIter->second.fStatusMsg;
        retStatus = extentEntryIter->second.fStatus;

        fMap.erase( extentEntryIter );
    }
    else // Allocate "stripe" of extents if there's no entry for this column OID
    {
        fStripeCount++;

        std::ostringstream oss1;
        oss1 << "Allocating next stripe(" << fStripeCount <<
            ") of column extents for table " << fTableOID <<
            "; DBRoot-" << dbRoot;
        fLog->logMsg( oss1.str(), MSGLVL_INFO2 );

        std::vector<BRM::CreateStripeColumnExtentsArgIn>  cols;
        std::vector<BRM::CreateStripeColumnExtentsArgOut> extents;
        for (unsigned int j=0; j<fColOIDs.size(); ++j)
        {
            BRM::CreateStripeColumnExtentsArgIn colEntry;
            colEntry.oid   = fColOIDs[j];
            colEntry.width = fColWidths[j];
            cols.push_back( colEntry );
        }

        uint32_t    allocPartNum   = partNum;
        uint16_t    allocSegNum    = 0;
        BRM::LBID_t allocStartLbid = 0;
        int         allocAllocSize = 0;
        HWM         allocHwm       = 0;
        int         allocStatus    = NO_ERROR;
        std::string allocStatusMsg;

        int rc = BRMWrapper::getInstance()->allocateStripeColExtents(
            cols, dbRoot, allocPartNum, allocSegNum, extents );

        // If allocation error occurs, we go ahead and store extent entries
        // with error status, to satisfy subsequent allocations in same stripe.
        if (rc != NO_ERROR)
        {
            for (unsigned int i=0; i<fColOIDs.size(); ++i)
            {
                if (oid != fColOIDs[i])
                {
                    allocStatus = rc;

                    std::ostringstream oss;
                    oss << "Previous error allocating extent stripe for "
                        "table " << fTableOID << "; DBRoot: " << dbRoot;
                    allocStatusMsg = oss.str();

                    // For error case, just store 0 for part#,segnum, etc.
                    AllocExtEntry extentEntry(fColOIDs[i], fColWidths[i],
                        dbRoot, 0, 0, 0, 0, 0,
                        allocStatus, allocStatusMsg, fStripeCount );

                    fMap.insert( AllocExtMap::value_type(fColOIDs[i],
                        extentEntry) );
                }
            }

            std::ostringstream oss;
            oss << "Error allocating extent stripe for "
                "table " << fTableOID << "; DBRoot: " << dbRoot;
            errMsg = oss.str();

            return rc;
        }

        // Save allocated extents into fMap for later use.  For the OID
        // requested by this function call, we just return the extent info.
        for (unsigned int i=0; i<fColOIDs.size(); ++i)
        {
            allocStartLbid = extents[i].startLbid;
            allocAllocSize = extents[i].allocSize;
            allocHwm       = extents[i].startBlkOffset;

            // Might consider controlling this with debug, but we always
            // log out for now.
            //if (fLog->isDebug( DEBUG_1 ))
            {
                std::ostringstream oss;
                oss << "Stripe Allocation: OID-"  << fColOIDs[i] <<
                    "; DBRoot-" << dbRoot         <<
                    "; Part#-"  << allocPartNum   <<
                    "; Seg#-"   << allocSegNum    <<
                    "; lbid-"   << allocStartLbid <<
                    "; fbo-"    << allocHwm       <<
                    "; nblks-"  << allocAllocSize;
                fLog->logMsg( oss.str(), MSGLVL_INFO2 );
            }

            // Assign output args for requested column OID
            if (oid == fColOIDs[i])
            {
                partNum   = allocPartNum;
                segNum    = allocSegNum;
                startLbid = allocStartLbid;
                allocSize = allocAllocSize;
                hwm       = allocHwm;
            }
            else // Add all extents in "stripe" (other than requested column)
            {    // to the collection of extents
                AllocExtEntry extentEntry(fColOIDs[i], fColWidths[i],
                    dbRoot, allocPartNum, allocSegNum,
                    allocStartLbid, allocAllocSize, allocHwm,
                    allocStatus, allocStatusMsg, fStripeCount );

                fMap.insert( AllocExtMap::value_type(fColOIDs[i],extentEntry) );
            }
        }
    }

    return retStatus;
}

//------------------------------------------------------------------------------
// Debug logging function to log contents of the allocated extents that are
// pending.
//------------------------------------------------------------------------------
void ExtentStripeAlloc::print( )
{
    boost::mutex::scoped_lock lock(fMapMutex);

    std::ostringstream oss;
    oss << "Current Pending Extents for table " << fTableOID << ":";

    if (fMap.size() > 0)
    {
        for (ConstAllocExtMapIter iter=fMap.begin();
             iter!= fMap.end();
             ++iter)
        {
            oss << std::endl;
            oss << "  oid: "    << iter->second.fOid       <<
                   "; wid: "    << iter->second.fColWidth  <<
                   "; root: "   << iter->second.fDbRoot    <<
                   "; part: "   << iter->second.fPartNum   <<
                   "; seg: "    << iter->second.fSegNum    <<
                   "; lbid: "   << iter->second.fStartLbid <<
                   "; size: "   << iter->second.fAllocSize <<
                   "; hwm: "    << iter->second.fHwm       <<
                   "; stripe: " << iter->second.fStripeKey <<
                   "; stat: "   << iter->second.fStatus    <<
                   "; msg: "    << iter->second.fStatusMsg;
        }
    }
    else
    {
        oss << " <EMPTY>";
    }

    fLog->logMsg( oss.str(), MSGLVL_INFO2 );
}

} //end of namespace
