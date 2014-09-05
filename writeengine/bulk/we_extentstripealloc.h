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
 * $Id: we_extentstripealloc.h 4450 2013-01-21 14:13:24Z rdempsey $
 *
 ******************************************************************************/

/** @file
 * Contains class to allocate a "stripe" of extents for all columns across a tbl
 */

#ifndef WE_EXTENTSTRIPEALLOC_H_
#define WE_EXTENTSTRIPEALLOC_H_

#include <string>
#include <vector>

#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif
#include <boost/thread/mutex.hpp>

#include "we_type.h"
#include "brmtypes.h"

namespace WriteEngine
{
    class Log;

//------------------------------------------------------------------------------
/** @brief Represents an extent allocation entry that is part of a "stripe".
 */
//------------------------------------------------------------------------------
class AllocExtEntry
{
public:
    // Default constructor
    AllocExtEntry() :   fOid(0),
                        fColWidth(0),
                        fDbRoot(0),
                        fPartNum(0),
                        fSegNum(0),
                        fStartLbid(0),
                        fAllocSize(0),
                        fHwm(0),
                        fStatus(NO_ERROR),
                        fStripeKey(0) { }

    // Used to create entry for an existing extent we are going to add data to.
    AllocExtEntry   (   OID& oid, int colWidth,
                        uint16_t dbRoot, uint32_t partNum, uint16_t segNum,
                        BRM::LBID_t startLbid, int allocSize,
                        HWM hwm, int status, const std::string& statusMsg,
                        unsigned int stripeKey ) :
                        fOid(oid),
                        fColWidth(colWidth),
                        fDbRoot(dbRoot),
                        fPartNum(partNum),
                        fSegNum(segNum),
                        fStartLbid(startLbid),
                        fAllocSize(allocSize),
                        fHwm(hwm),
                        fStatus(status),
                        fStatusMsg(statusMsg),
                        fStripeKey(stripeKey) { }

    OID         fOid;       // column OID
    int         fColWidth;  // colum width (in bytes)
    uint16_t    fDbRoot;    // DBRoot of allocated extent
    uint32_t    fPartNum;   // Partition number of allocated extent
    uint16_t    fSegNum;    // Segment number of allocated extent
    BRM::LBID_t fStartLbid; // Starting LBID of allocated extent
    int         fAllocSize; // Number of allocated LBIDS
    HWM         fHwm;       // Starting fbo or hwm of allocated extent
    int         fStatus;    // Status of extent allocation
    std::string fStatusMsg; // Status msg of extent allocation
    unsigned int fStripeKey;// "Stripe" identifier for this extent
};

//------------------------------------------------------------------------------
/** @brief Hash function used to store AllocExtEntry objects into a map; using
 *  the corresponding column OID as the key.
 */
//------------------------------------------------------------------------------
struct AllocExtHasher : public std::unary_function<OID,std::size_t>
{
    std::size_t operator()(OID val) const
    { return static_cast<std::size_t>(val); }
};

//------------------------------------------------------------------------------
/** @brief Manages allocation of a "stripe" of column extents across a table.
 *  Allocates and stores the allocated extent information till each extent is
 *  requested by one of the parsing threads.
 */
//------------------------------------------------------------------------------
class ExtentStripeAlloc
{
public:

    /** @brief Constructor
     *  @param tableOID OID of table for which extents will be allocated.
     *  @param logger Log object used for debug logging.
     */
    ExtentStripeAlloc ( OID tableOID,
                        Log* logger );

    /** @brief Destructor
     */
    ~ExtentStripeAlloc( );

    /** @brief Add the specified column to our "stripe" of extents to allocate.
     *  @param colOID   Column OID to be added to extent allocation list.
     *  @param colWidth Width of column associated with colOID.
     */
    void addColumn( OID colOID,
                    int colWidth );

    /** @brief Request an extent allocation for the specified OID and DBRoot.
     *  A "stripe" of extents for the corresponding table will be allocated
     *  if no extent exists to satisfy the request; else the previously
     *  allocated extent will be returned.
     *  @param oid       Column OID extent to be allocated
     *  @param dbRoot    Requested DBRoot for the new extent
     *  @param partNum   (in/out) Partition number of the allocated extent,
     *                         Input if empty DBRoot, else only used as output.
     *  @param segNum    (out) Segment number of the allocated extent
     *  @param startLbid (out) Starting LBID of the allocated extent
     *  @param allocSize (out) Number of blocks in the allocated extent
     *  @param hwm       (out) Starting FBO or hwm for the allocated extent
     *  @param errMsg    (out) Error msg associated with the extent allocation
     *  @return NO_ERROR returned upon success
     */
    int  allocateExtent( OID        oid,
                    uint16_t        dbRoot,
                    uint32_t&       partNum,
                    uint16_t&       segNum,
                    BRM::LBID_t&    startLbid,
                    int&            allocSize,
                    HWM&            hwm,
                    std::string&    errMsg );

    /** @brief Debug print function.
     */
    void print( );

private:
    OID                 fTableOID;      // Table extents to be allocated
    Log*                fLog;           // Log used for debug logging
    unsigned int        fStripeCount;   // Extent "stripe" counter
    boost::mutex        fMapMutex;      // protects unordered map access
    std::vector<OID>    fColOIDs;       // Vector of column OIDs
    std::vector<int>    fColWidths;     // Widths associated with fColOIDs

    // unordered map where we collect the allocated extents
    std::tr1::unordered_multimap<OID,AllocExtEntry,AllocExtHasher> fMap;

    // disable copy constructor and assignment operator
    ExtentStripeAlloc(const ExtentStripeAlloc&);
    ExtentStripeAlloc& operator=(const ExtentStripeAlloc&);
};

} //end of namespace

#endif // WE_EXTENTSTRIPEALLOC_H_
