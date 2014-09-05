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
* $Id: we_dbrootextenttracker.cpp 4450 2013-01-21 14:13:24Z rdempsey $
*/
#define WRITEENGINEDBEXTTRK_DLLEXPORT
#include "we_dbrootextenttracker.h"
#undef WRITEENGINEDBEXTTRK_DLLEXPORT

#include <algorithm>
#include <sstream>

#include "we_brm.h"
#include "we_config.h"
#include "we_log.h"

namespace
{
    const char* stateStrings[] = { "initState"     ,
                                   "PartialExtent" ,
                                   "EmptyDbRoot"   ,
                                   "ExtentBoundary" };
}

namespace WriteEngine
{

//------------------------------------------------------------------------------
// DBRootExtentInfo constructor
//------------------------------------------------------------------------------
DBRootExtentInfo::DBRootExtentInfo(
    uint16_t    dbRoot,
    uint32_t    partition,
    uint16_t    segment,
    BRM::LBID_t startLbid,
    HWM         localHwm,
    uint64_t    dbrootTotalBlocks,
    DBRootExtentInfoState state) :
    fPartition(partition),
    fDbRoot(dbRoot),
    fSegment(segment),
    fStartLbid(startLbid),
    fLocalHwm(localHwm),
    fDBRootTotalBlocks(dbrootTotalBlocks),
    fState(state)
{
}

//------------------------------------------------------------------------------
// LessThan operator used to sort DBRootExtentInfo objects by DBRoot.
//------------------------------------------------------------------------------
bool DBRootExtentInfo::operator<(
    const DBRootExtentInfo& entry) const
{
    if (fDbRoot < entry.fDbRoot)
        return true;
    return false;
}

//------------------------------------------------------------------------------
// DBRootExtentTracker constructor
//
// Mutex lock not needed in this function as it is only called from main thread
// before processing threads are spawned.
//------------------------------------------------------------------------------
DBRootExtentTracker::DBRootExtentTracker ( OID oid,
    const std::vector<int>& colWidths,
    const std::vector<BRM::EmDbRootHWMInfo_v>& dbRootHWMInfoColVec,
    unsigned int columnIdx,
    Log* logger ) :
    fOID(oid),
    fLog(logger),
    fCurrentDBRootIdx(-1),
    fStartedWithEmptyPM(false),
    fEmptyPMFirstDbRoot(0)
{
    const BRM::EmDbRootHWMInfo_v& emDbRootHWMInfo =
        dbRootHWMInfoColVec[columnIdx];
    int colWidth = colWidths[columnIdx];

    fBlksPerExtent = (long long)BRMWrapper::getInstance()->getExtentRows() *
        (long long)colWidth / (long long)BYTE_PER_BLOCK;

    std::vector<bool> resetState;
    for (unsigned int i=0; i<emDbRootHWMInfo.size(); i++)
    {
        resetState.push_back(false);
        DBRootExtentInfoState state = determineState(
            colWidths[columnIdx],
            emDbRootHWMInfo[i].localHWM,
            emDbRootHWMInfo[i].totalBlocks);

        // For a full extent...
        // check to see if any of the column HWMs are partially full, in which
        // case we consider all the columns for that DBRoot to be partially
        // full.  (This can happen if a table has columns with varying widths,
        // as the HWM may be at the last extent block for a shorter column, and
        // still have free blocks for wider columns.)
        if (state == DBROOT_EXTENT_EXTENT_BOUNDARY)
        {
            for (unsigned int kCol=0; kCol<dbRootHWMInfoColVec.size(); kCol++)
            {
                const BRM::EmDbRootHWMInfo_v& emDbRootHWMInfo2 =
                    dbRootHWMInfoColVec[kCol];
                DBRootExtentInfoState state2 = determineState(
                    colWidths[kCol],
                    emDbRootHWMInfo2[i].localHWM,
                    emDbRootHWMInfo2[i].totalBlocks);
                if (state2 == DBROOT_EXTENT_PARTIAL_EXTENT)
                {
                    state = DBROOT_EXTENT_PARTIAL_EXTENT;
                    resetState[ resetState.size()-1 ] = true;
                    break;
                }
            }
        }

        DBRootExtentInfo dbRootExtent(
            emDbRootHWMInfo[i].dbRoot,
            emDbRootHWMInfo[i].partitionNum,
            emDbRootHWMInfo[i].segmentNum,
            emDbRootHWMInfo[i].startLbid,
            emDbRootHWMInfo[i].localHWM,
            emDbRootHWMInfo[i].totalBlocks,
            state);

        fDBRootExtentList.push_back( dbRootExtent );
    }

    std::sort( fDBRootExtentList.begin(), fDBRootExtentList.end() );

    if (fLog)
    {
        // Always log this info for now; may control with debug later
        //if (fLog->isDebug(DEBUG_1))
        {
            std::ostringstream oss;
            oss << "Starting DBRoot info for OID " << fOID;
            for (unsigned int k=0; k<fDBRootExtentList.size(); k++)
            {
                oss << std::endl;
                oss << "  DBRoot-" << fDBRootExtentList[k].fDbRoot <<
                    ", part/seg/hwm/LBID/totBlks/state: "          <<
                    fDBRootExtentList[k].fPartition                <<
                    "/" << fDBRootExtentList[k].fSegment           <<
                    "/" << fDBRootExtentList[k].fLocalHwm          <<
                    "/" << fDBRootExtentList[k].fStartLbid         <<
                    "/" << fDBRootExtentList[k].fDBRootTotalBlocks <<
                    "/" << stateStrings[ fDBRootExtentList[k].fState ];
                if (resetState[k])
                    oss << ".";
            }
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }
    }
}

//------------------------------------------------------------------------------
// Determines the state of the HWM extent (for a DBRoot); considering the
// current BRM status, HWM, and total block count for the DBRoot.
//------------------------------------------------------------------------------
DBRootExtentInfoState DBRootExtentTracker::determineState(int colWidth,
    HWM      localHwm,
    uint64_t dbRootTotalBlocks)
{
    DBRootExtentInfoState extentState;

    if (dbRootTotalBlocks == 0)
    {
        extentState = DBROOT_EXTENT_EMPTY_DBROOT;
    }
    else
    {
        extentState = DBROOT_EXTENT_PARTIAL_EXTENT;

        // See if local hwm is on an extent bndry,in which case the extent
        // is full and we won't be adding rows to the current HWM extent;
        // we will instead need to allocate a new extent in order to begin
        // adding any rows.
        long long nRows= ((long long)(localHwm+1) *
                          (long long)BYTE_PER_BLOCK)/ (long long)colWidth;
        long long nRem = nRows % BRMWrapper::getInstance()->getExtentRows();
        if (nRem == 0)
        {
            extentState = DBROOT_EXTENT_EXTENT_BOUNDARY;
        }
    }

    return extentState;
}

//------------------------------------------------------------------------------
// Select the first segment file to add rows to for the local PM.
// Function will first try to find the HWM extent with the fewest blocks.
// If all the HWM extents are equal, then we select the DBRoot/segment file
// with the fewest total overall blocks for that DBRoot.
// Return value is 0 upon success, else non zero if no eligible entries found.
//
// Mutex lock not needed in this function as it is only called from main thread
// before processing threads are spawned.
//------------------------------------------------------------------------------
int DBRootExtentTracker::selectFirstSegFile(
    DBRootExtentInfo& dbRootExtent, bool& bFirstExtentOnThisPM,
    std::string& errMsg )
{
    int startExtentIdx       = -1;
    int fewestLocalBlocksIdx = -1; // track HWM extent with fewest blocks
    int fewestTotalBlocksIdx = -1; // track DBRoot with fewest total blocks
    bFirstExtentOnThisPM     = false;

    unsigned int fewestTotalBlks = UINT_MAX;
    unsigned int fewestLocalBlks = UINT_MAX;
    uint16_t     fewestTotalBlkSegNum = USHRT_MAX;
    uint16_t     fewestLocalBlkSegNum = USHRT_MAX;

    // Find DBRoot having HWM extent with fewest blocks.  If all HWM extents
    // are equal, then fall-back on selecting the DBRoot with fewest total blks.
    //
    // Selecting HWM extent with fewest blocks should be straight forward, be-
    // cause all the DBRoots on a PM should end on an extent boundary except
    // for the current last extent.  But if the user has moved a DBRoot, then
    // we can end up with 2 partially filled HWM extents on 2 DBRoots, on the
    // same PM.  That's why we loop through the DBRoots to see if we have more
    // than 1 partially filled HWM extent.
    for (unsigned int iroot=0;
         iroot<fDBRootExtentList.size();
         iroot++)
    {
        // Skip over DBRoots which have no extents
        if (fDBRootExtentList[iroot].fDBRootTotalBlocks == 0)
            continue;

        // Find DBRoot and segment file with most incomplete extent.
        // Break a tie by selecting the lowest segment number.
        long long remBlks = (long long)(fDBRootExtentList[iroot].fLocalHwm + 1)%
            fBlksPerExtent;
        if (remBlks > 0)
        {
            if ( (remBlks <  fewestLocalBlks) ||
                ((remBlks == fewestLocalBlks) &&
                 (fDBRootExtentList[iroot].fSegment < fewestLocalBlkSegNum)) )
            {
                fewestLocalBlocksIdx = iroot;
                fewestLocalBlks      = remBlks;
                fewestLocalBlkSegNum = fDBRootExtentList[iroot].fSegment;
            }
        }

        // Find DBRoot with fewest total of blocks.
        // Break a tie by selecting the highest segment number.
        if ( (fDBRootExtentList[iroot].fDBRootTotalBlocks < fewestTotalBlks) ||
            ((fDBRootExtentList[iroot].fDBRootTotalBlocks== fewestTotalBlks) &&
             (fDBRootExtentList[iroot].fSegment > fewestTotalBlkSegNum)) )
        {
            fewestTotalBlocksIdx = iroot;
            fewestTotalBlks      = fDBRootExtentList[iroot].fDBRootTotalBlocks;
            fewestTotalBlkSegNum = fDBRootExtentList[iroot].fSegment;
        }
    }

    // Select HWM extent with fewest number of blocks
    if (fewestLocalBlocksIdx != -1)
    {
        startExtentIdx = fewestLocalBlocksIdx;
    }

    // Select DBRoot with the fewest total number of blocks
    else if (fewestTotalBlocksIdx != -1)
    {
        startExtentIdx = fewestTotalBlocksIdx;
    }

    // PM with no extents, so select DBRoot/segment file from DBRoot list, where
    // we will start inserting rows.  Select lowest segment file number.
    else
    {
        RETURN_ON_ERROR( selectFirstSegFileForEmptyPM( errMsg ) );

        startExtentIdx   = fCurrentDBRootIdx;
    }

    bFirstExtentOnThisPM = fStartedWithEmptyPM;
    fCurrentDBRootIdx    = startExtentIdx;

    // Finish Initializing DBRootExtentList for empty DBRoots w/o any extents
    initEmptyDBRoots( );

    logFirstDBRootSelection( );

    dbRootExtent = fDBRootExtentList[startExtentIdx];
    fDBRootExtentList[startExtentIdx].fState = DBROOT_EXTENT_EXTENT_BOUNDARY;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// If we have encountered a PM with no extents, then this function can be
// called to determine the DBRoot to be used for the first extent for the
// applicable PM.  First DBRoot for relevant PM is selected.  At extent
// creation time, BRM will assign the segment number.
//
// Mutex lock not needed in this function as it is only called from main thread
// before processing threads are spawned.
//------------------------------------------------------------------------------
int DBRootExtentTracker::selectFirstSegFileForEmptyPM( std::string& errMsg )
{
    fStartedWithEmptyPM = true;

    fCurrentDBRootIdx   = 0;    // Start with first DBRoot for this PM
    fEmptyPMFirstDbRoot = fDBRootExtentList[fCurrentDBRootIdx].fDbRoot;

    // Always start empty PM with partition number 0.  If PM DBRoots were empty
    // then fPartition should already be 0, but explicitly set just to be safe.
    fDBRootExtentList[fCurrentDBRootIdx].fPartition = 0;

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Finish Initializing fDBRootExtentList for any empty DBRoots w/o any extents.
//------------------------------------------------------------------------------
void DBRootExtentTracker::initEmptyDBRoots( )
{
    int startExtentIdx= fCurrentDBRootIdx;
    bool bAnyChanges  = false; // If fDBRootExtentList changes, log the contents

    // Fill in starting partition for any DBRoots having no extents
    for (unsigned int iroot=0;
         iroot<fDBRootExtentList.size();
         iroot++)
    {
        if ((fDBRootExtentList[iroot].fDBRootTotalBlocks == 0) &&
            ((int)iroot != startExtentIdx)) // skip over selected dbroot
        {
            if (fDBRootExtentList[iroot].fPartition !=
                fDBRootExtentList[startExtentIdx].fPartition)
            {
                bAnyChanges = true;

                fDBRootExtentList[iroot].fPartition = 
                    fDBRootExtentList[startExtentIdx].fPartition;
            }
        }
    }

    // Log fDBRootExtentList if modifications were made
    if ((bAnyChanges) && (fLog))
    {
        // Always log this info for now; may control with debug later
        //if (fLog->isDebug(DEBUG_1))
        {
            std::ostringstream oss;
            oss << "Updated starting (empty) DBRoot info for OID " << fOID;
            for (unsigned int k=0; k<fDBRootExtentList.size(); k++)
            {
                oss << std::endl;
                oss << "  DBRoot-" << fDBRootExtentList[k].fDbRoot <<
                    ", part/seg/hwm/LBID/totBlks/state: "          <<
                    fDBRootExtentList[k].fPartition                <<
                    "/" << fDBRootExtentList[k].fSegment           <<
                    "/" << fDBRootExtentList[k].fLocalHwm          <<
                    "/" << fDBRootExtentList[k].fStartLbid         <<
                    "/" << fDBRootExtentList[k].fDBRootTotalBlocks <<
                    "/" << stateStrings[ fDBRootExtentList[k].fState ];
            }
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }
    }
}

//------------------------------------------------------------------------------
// Assign the DBRoot/segment file to be used for extent loading based on the
// setting in the specified reference tracker.
//
// Mutex lock not needed in this function as it is only called from main thread
// before processing threads are spawned.
//------------------------------------------------------------------------------
void DBRootExtentTracker::assignFirstSegFile(
    const DBRootExtentTracker& refTracker,
    DBRootExtentInfo& dbRootExtent)
{
    // Start with the same DBRoot index as the reference tracker; assumes that
    // DBRoots for each column are listed in same order in fDBRootExtentList.
    // That should be a safe assumption since DBRootExtentTracker constructor
    // sorts the entries in fDBRootExtentList by fDbRoot.
    int startExtentIdx   = refTracker.fCurrentDBRootIdx;
    fStartedWithEmptyPM  = refTracker.fStartedWithEmptyPM;
    fEmptyPMFirstDbRoot  = refTracker.fEmptyPMFirstDbRoot;
  
    // For an empty PM, we pick up the DBRoot selected and saved in the
    // reference tracker.
    if (fStartedWithEmptyPM)
    {
        DBRootExtentInfo dbRootExtent2(
            refTracker.fDBRootExtentList[startExtentIdx].fDbRoot,
            0      , // always start empty PM with partition number 0
            0      , // segment number (n/a)
            0      , // starting LBID  (n/a)
            0      , // local HWM      (n/a)
            0      , // total blocks for this dbroot
            DBROOT_EXTENT_EMPTY_DBROOT);
        fDBRootExtentList[startExtentIdx] = dbRootExtent2;
    }

    fCurrentDBRootIdx    = startExtentIdx;

    // Finish Initializing DBRootExtentList for empty DBRoots w/o any extents
    initEmptyDBRoots( );

    logFirstDBRootSelection( );

    dbRootExtent = fDBRootExtentList[startExtentIdx];
    fDBRootExtentList[startExtentIdx].fState = DBROOT_EXTENT_EXTENT_BOUNDARY;
}

//------------------------------------------------------------------------------
// Log information about the first DBRoot/segment file that is selected.
//------------------------------------------------------------------------------
void DBRootExtentTracker::logFirstDBRootSelection( ) const
{
    if (fLog)
    {
        int extentIdx = fCurrentDBRootIdx;

        if (fStartedWithEmptyPM)
        {
            std::ostringstream oss;
            oss << "Will be creating first segFile on this PM for oid-" <<fOID<<
                "; DBRoot-" << fDBRootExtentList[extentIdx].fDbRoot <<
                ", part/seg/state: " <<fDBRootExtentList[extentIdx].fPartition<<
                "/" << fDBRootExtentList[extentIdx].fSegment   <<
                "/" << stateStrings[ fDBRootExtentList[extentIdx].fState ] <<
                "; Based on first seg being on DBRoot" << fEmptyPMFirstDbRoot;
                fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }
        else
        {
            std::ostringstream oss;
            oss<<"Selecting existing segFile to begin adding rows: oid-"<<fOID<<
                "; DBRoot-" << fDBRootExtentList[extentIdx].fDbRoot   <<
                ", part/seg/hwm/LBID/totBlks/state: "                 <<
                fDBRootExtentList[extentIdx].fPartition               <<
                "/" << fDBRootExtentList[extentIdx].fSegment          <<
                "/" << fDBRootExtentList[extentIdx].fLocalHwm         <<
                "/" << fDBRootExtentList[extentIdx].fStartLbid        <<
                "/" << fDBRootExtentList[extentIdx].fDBRootTotalBlocks<<
                "/" << stateStrings[ fDBRootExtentList[extentIdx].fState ];
            fLog->logMsg( oss.str(), MSGLVL_INFO2 );
        }
    }
}

//------------------------------------------------------------------------------
// Iterate/return next DBRoot to be used for the next extent
// If it is the "very" 1st extent for the specified DBRoot, then the applicable
// partition and segment number to be used for the first extent, are also
// returned.  If a partial extent is to be filled in, then the current HWM
// and starting LBID for the relevant extent are returned.
// Returns true if new extent needs to be allocated, else false indicates that
// the extent is partially full.
//------------------------------------------------------------------------------
bool DBRootExtentTracker::nextSegFile(
    uint16_t&    dbRoot,
    uint32_t&    partition,
    uint16_t&    segment,
    HWM&         localHwm,
    BRM::LBID_t& startLbid)
{
    boost::mutex::scoped_lock lock(fDBRootExtTrkMutex);

    fCurrentDBRootIdx++;
    if ((unsigned int)fCurrentDBRootIdx >= fDBRootExtentList.size())
        fCurrentDBRootIdx = 0;
    dbRoot    = fDBRootExtentList[fCurrentDBRootIdx].fDbRoot;
    segment   = fDBRootExtentList[fCurrentDBRootIdx].fSegment;
    partition = fDBRootExtentList[fCurrentDBRootIdx].fPartition;
    localHwm  = fDBRootExtentList[fCurrentDBRootIdx].fLocalHwm;
    startLbid = fDBRootExtentList[fCurrentDBRootIdx].fStartLbid;
//std::cout << "NextSegFile: Current idx: " << fCurrentDBRootIdx <<
//"; new dbroot: " << dbRoot    <<
//"; segment: "    << segment   <<
//"; partition: "  << partition <<
//"; localHwm: "   << localHwm  <<
//"; startLbid: "  << startLbid <<
//"; state: "      << stateStrings[fDBRootExtentList[fCurrentDBRootIdx].fState]
//  << std::endl;

    bool bAllocExtentFlag = true;
    if (fDBRootExtentList[fCurrentDBRootIdx].fState ==
        DBROOT_EXTENT_PARTIAL_EXTENT)
        bAllocExtentFlag = false;

    // After we have taken care of the "first" extent for each DBRoot, we can
    // zero out everything.  The only thing we need to continue rotating thru
    // the DBRoots, is the DBRoot number itself.
    fDBRootExtentList[fCurrentDBRootIdx].fSegment           = 0;
    fDBRootExtentList[fCurrentDBRootIdx].fPartition         = 0;
    fDBRootExtentList[fCurrentDBRootIdx].fLocalHwm          = 0;
    fDBRootExtentList[fCurrentDBRootIdx].fStartLbid         = 0;
    fDBRootExtentList[fCurrentDBRootIdx].fDBRootTotalBlocks = 0;
    fDBRootExtentList[fCurrentDBRootIdx].fState = DBROOT_EXTENT_EXTENT_BOUNDARY;

    return bAllocExtentFlag;
}

const std::vector<DBRootExtentInfo>& DBRootExtentTracker::getDBRootExtentList()
{
    boost::mutex::scoped_lock lock(fDBRootExtTrkMutex);
    return fDBRootExtentList;
}

} // end of namespace
