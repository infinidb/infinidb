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
* $Id: we_dbrootextenttracker.h 4631 2013-05-02 15:21:09Z dcathey $
*/

/** @file we_dbrootextenttracker.h
 * Contains classes to track the order of placement (rotation) of extents as
 * they are filled in and/or added to the DBRoots for the local PM.
 *
 * DBRootExtentTracker did select the next DBRoot and segment number when-
 * ever either selectFirstSegFile() or nextSegFile() were called.  The logic
 * for selecting a "new" segment file number previously in nextSegFile() has
 * been moved to the DBRM extent allocation function.  The segment number
 * argument returned by nextSegFile() is now only applicable if the return
 * value is false, indicating that a partially filled extent has been en-
 * countered.
 */

#ifndef WE_DBROOTEXTENTTRACKER_H_
#define WE_DBROOTEXTENTTRACKER_H_

#include <boost/thread/mutex.hpp>
#include <vector>

#include "we_type.h"
#include "brmtypes.h"

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace WriteEngine
{
    class Log;

//
// PARTIAL_EXTENT - Extent is partially filled
// EMPTY_DBROOT   - DRoot is empty (has no extents)
// EXTENT_BOUNDARY- Encountered extent boundary, add next extent
// OUT_OF_SERVICE - Extent is disabled or out-of-service
//
// Changes to this enum should be reflected in stateStrings array in
// we_dbrootextenttracker.cpp.
//
enum DBRootExtentInfoState
{
    DBROOT_EXTENT_PARTIAL_EXTENT  = 1,
    DBROOT_EXTENT_EMPTY_DBROOT    = 2,
    DBROOT_EXTENT_EXTENT_BOUNDARY = 3,
    DBROOT_EXTENT_OUT_OF_SERVICE  = 4
};

//------------------------------------------------------------------------------
/** @brief Tracks the current DBRoot/extent we are loading.
 */
//------------------------------------------------------------------------------
struct DBRootExtentInfo
{
    uint32_t              fPartition;
    uint16_t              fDbRoot;
    uint16_t              fSegment;
    BRM::LBID_t           fStartLbid;
    HWM                   fLocalHwm;
    uint64_t              fDBRootTotalBlocks;
    DBRootExtentInfoState fState;

    DBRootExtentInfo() :
        fPartition(0),
        fDbRoot(0),
        fSegment(0),
        fStartLbid(0),
        fLocalHwm(0),
        fDBRootTotalBlocks(0),
        fState(DBROOT_EXTENT_PARTIAL_EXTENT) { }

    DBRootExtentInfo(
        uint16_t    dbRoot,
        uint32_t    partition,
        uint16_t    segment,
        BRM::LBID_t startLbid,
        HWM         localHwm,
        uint64_t    dbrootTotalBlocks,
        DBRootExtentInfoState state);

    bool operator<(const DBRootExtentInfo& entry) const;
};

//------------------------------------------------------------------------------
/** @brief Class to track the order of placement (rotation) of extents as
 *  they are filled in and/or added to the DBRoots for the relevant PM.
 */
//------------------------------------------------------------------------------
class DBRootExtentTracker
{
public:

    /** @brief DBRootExtentTracker constructor
     * @param oid Column OID of interest.
     * @param colWidths Widths (in bytes) of all the columns in the table.
     * @param dbRootHWMInfoColVec Column HWM, DBRoots, etc for this table.
     * @param columnIdx Index (into colWidths and dbRootHWMInfoColVec)
     *        referencing the column that applies to this ExtentTracker.
     * @param logger Logger to be used for logging messages.
     */
    EXPORT DBRootExtentTracker ( OID oid,
        const std::vector<int>& colWidths,
        const std::vector<BRM::EmDbRootHWMInfo_v>& dbRootHWMInfoColVec,
        unsigned int columnIdx,
        Log* logger );

    /** @brief Select the first DBRoot/segment file to add rows to, for this PM.
     * @param dbRootExtent Dbroot/segment file selected for first set of rows.
     * @param bNoStartExtentOnThisPM Is starting HWM extent missing or disabled.
     *        If HWM extent is missing or disabled, the app will have to allo-
     *        cate a new extent (at the DBRoot returned in dbRootExtent)) in
     *        order to add any rows.
     * @param bEmptyPM  Is this PM void of any available extents
     * @return Returns NO_ERROR if success, else returns error code.
     */
    EXPORT int selectFirstSegFile ( DBRootExtentInfo& dbRootExtent,
                             bool& bNoStartExtentOnThisPM,
                             bool& bEmptyPM,
                             std::string& errMsg );

    /** @brief Set up this Tracker to select the same first DBRoot/segment file
     * as the reference DBRootExtentTracker that is specified from a ref column.
     *
     * Application code should call selectFirstSegFile for a reference column,
     * and assignFirstSegFile for all other columns in the same table.
     * @param refTracker Tracker object used to assign first DBRoot/segment.
     * @param dbRootExtent Dbroot/segment file selected for first set of rows.
     */
    EXPORT void assignFirstSegFile( const DBRootExtentTracker& refTracker,
                             DBRootExtentInfo& dbRootExtent );

    /** @brief Iterate/return next DBRoot to be used for the next extent.
     *
     * Case 1)
     * If it is the "very" first extent for the specified DBRoot, then the
     * applicable partition to be used for the first extent is also returned.
     *
     * Case 2)
     * If the user moves a DBRoot to a different PM, then the next cpimport.bin
     * job on the recepient PM may encounter 2 partially filled in extents.
     * This differs from the norm, where we only have 1 partially filled extent
     * at any given time, on a PM.  When a DBRoot is moved, we may finish an ex-
     * tent on 1 DBRoot, and instead of advancing to start a new extent, we ro-
     * tate to the recently moved DBRoot, and have to first fill in a partilly
     * filled in extent instead of adding a new extent.  Case 2 is intended to
     * cover this use case.
     * In this case, in the middle of an import, if the next extent to receive
     * rows is a partially filled in extent, then the DBRoot, partition, and 
     * segment number for the partial extent are returned.  In addition, the
     * current HWM and starting LBID for the relevant extent are returned.
     *
     * Case 3)
     * If we are just finishing one extent and adding the next extent, then
     * only the DBRoot argument is relevant, telling us where to create the
     * next extent.  Return value will be true.  This case also applies to
     * the instance where the HWM extent for the next DBRoot is disabled.
     *
     * @param dbRoot DBRoot for the next extent
     * @param partition If first extent on dbRoot (or partial extent), then
     *        this is the partition #
     * @param segment If partially full extent, then this is the segment #
     * @param localHwm If partially full extent, then this is current HWM.
     * @param startLbid If partially full extent, then this is starting LBID of
     *         the current HWM extent.
     *
     * @return Returns true if new extent needs to be allocated, returns false
     *         if extent is partially full, and has room for more rows.
     */
    EXPORT bool nextSegFile( uint16_t&  dbRoot,
                    uint32_t&    partition,
                    uint16_t&    segment,
                    HWM&         localHwm,
                    BRM::LBID_t& startLbid );

    /** @brief get the DBRootExtentInfo list
     */
    const std::vector<DBRootExtentInfo>& getDBRootExtentList();

    /** @brief get the CurrentDBRootIdx
     */
    inline const int getCurrentDBRootIdx()
    {
        boost::mutex::scoped_lock lock(fDBRootExtTrkMutex);
        return fCurrentDBRootIdx;
    }

private:
    DBRootExtentInfoState determineState(int colWidth,
        HWM      localHwm,
        uint64_t dbRootTotalBlocks,
        int16_t  status);
    // Select First DBRoot/segment file on a PM having no extents for fOID
    int  selectFirstSegFileForEmptyPM ( std::string& errMsg );
    void initEmptyDBRoots();                // init ExtentList for empty DBRoots
    void logFirstDBRootSelection() const;

    OID             fOID;                   // applicable colunn OID
    long long       fBlksPerExtent;         // blocks per extent for fOID
    Log*            fLog;                   // logger
    boost::mutex    fDBRootExtTrkMutex;     // mutex to access fDBRootExtentList
    int             fCurrentDBRootIdx;      // Index into fDBRootExtentList,
                                            //   DBRoot where current extent is
                                            //   being added
    std::vector<DBRootExtentInfo> fDBRootExtentList; // List of current pending
                                            //   DBRoot/extents for each DBRoot
                                            //   assigned to the local PM.
    bool            fEmptyOrDisabledPM;     // true if PM has no extents or all
                                            //   extents are disabled
    bool            fEmptyPM;               // true if PM has no available or
                                            //   disabled extents
    bool            fDisabledHWM;           // Did job start with disabled HWM 
};

} //end of namespace

#undef EXPORT

#endif // WE_DBROOTEXTENTTRACKER_H_
