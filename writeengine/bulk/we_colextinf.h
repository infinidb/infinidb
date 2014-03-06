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
 * $Id: we_colextinf.h 4501 2013-01-31 21:15:58Z dcathey $
 *
 ******************************************************************************/

/** @file
 * Contains class to track column information per extent.
 * For ex: this is where we track the min/max values per extent for a column.
 */

#ifndef WE_COLEXTINF_H_
#define WE_COLEXTINF_H_

#include <limits>
#include <stdint.h>
#include <set>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif
#include <boost/thread/mutex.hpp>

#include "brmtypes.h"
#include "we_type.h"

namespace WriteEngine
{
    class Log;
    class BRMReporter;
    typedef execplan::CalpontSystemCatalog::ColDataType ColDataType;
//------------------------------------------------------------------------------
/** @brief Class to store min/max and LBID information for an extent.
 *  For character data, the min and max values are maintained in reverse
 *  order to facilitate string comparisions.  When the range is sent to
 *  BRM, the bytes will be swapped back into the correct order.
 *  BRM will need to be told when the column carries character data, so
 *  that BRM can do the correct binary comparisons of the char data.
 */
//------------------------------------------------------------------------------
class ColExtInfEntry
{
public:
    // Default constructor
    ColExtInfEntry() : fLbid(INVALID_LBID),
                       fMinVal(LLONG_MIN),
                       fMaxVal(LLONG_MIN),
                       fNewExtent(true)   { }

    // Used to create entry for an existing extent we are going to add data to.
    ColExtInfEntry(BRM::LBID_t lbid, bool bIsNewExtent) :
                       fLbid(lbid),
                       fMinVal(LLONG_MIN),
                       fMaxVal(LLONG_MIN),
                       fNewExtent(bIsNewExtent)  { }

    // Used to create entry for a new extent, with LBID not yet allocated
    ColExtInfEntry(int64_t minVal, int64_t maxVal) :
                       fLbid(INVALID_LBID),
                       fMinVal(minVal),
                       fMaxVal(maxVal),
                       fNewExtent(true)   { }

    // Used to create entry for a new extent, with LBID not yet allocated
    ColExtInfEntry(uint64_t minVal, uint64_t maxVal) :
                       fLbid(INVALID_LBID),
                       fMinVal(static_cast<int64_t>(minVal)),
                       fMaxVal(static_cast<int64_t>(maxVal)),
                       fNewExtent(true)   { }

    BRM::LBID_t fLbid;     // LBID for an extent; should be the starting LBID
    int64_t     fMinVal;   // minimum value for extent associated with LBID
    int64_t     fMaxVal;   // maximum value for extent associated with LBID 
    bool        fNewExtent;// is this a new extent
};

//------------------------------------------------------------------------------
/** @brief Hash function used to store ColEntInfEntry objects into a map; using
 *  the last input Row number in the extent, as the key.
 */
//------------------------------------------------------------------------------
struct uint64Hasher : public std::unary_function<RID,std::size_t>
{
    std::size_t operator()(RID val) const
    { return static_cast<std::size_t>(val); }
};

//------------------------------------------------------------------------------
/** @brief Stub base class for ColExtInf; used for column data types that do
 *  not need the functionality of ColExtInf (ex: floats and dictionaries).
 */
//------------------------------------------------------------------------------
class ColExtInfBase
{
public:
    ColExtInfBase( )                                        { }
    virtual ~ColExtInfBase( )                               { }

    virtual void addFirstEntry   ( RID     lastInputRow,
                                   BRM::LBID_t lbid,
                                   bool    bIsNewExtent)    { }

    virtual void addOrUpdateEntry( RID     lastInputRow,
                                   int64_t minVal,
                                   int64_t maxVal,
                                   ColDataType colDataType ){ }

    virtual void getCPInfoForBRM ( JobColumn column,
                                   BRMReporter& brmReporter){ }
    virtual void print( const JobColumn& column )           { }
    virtual int updateEntryLbid( BRM::LBID_t startLbid )    { return NO_ERROR; }
};

//------------------------------------------------------------------------------
/** @brief Collects LBID and min/max info about the extents that are loaded.
 *
 *  As a Read buffer is parsed, addOrUpdateEntryi() is called to add the extent,
 *  and it's information to the collection.  For new extents, we have to add
 *  the LBID later, when the extent is allocated, since the extent's first
 *  buffer will be finished before the extent is allocated from BRM.  In this
 *  case, updateEntryLbid() is called to add the LBID.  The specified LBID is
 *  assigned to the extent with the lowest Row id that is awaiting an LBID.
 *  This should be a safe assumption to make, that the extents will be allocated
 *  in Row id order.   lastInputRow numbers are relative to the first row in
 *  the import (ie: Row 0 is the first row in the *.tbl file).
 */
//------------------------------------------------------------------------------
class ColExtInf : public ColExtInfBase
{
public:

    /** @brief Constructor
     *  @param logger Log object using for debug logging.
     */
    ColExtInf( OID oid, Log* logger ) : fColOid(oid), fLog(logger) { }
    virtual ~ColExtInf( )                   { }

    /** @brief Add an entry for first extent, for the specified Row and LBID.
     *  @param lastInputRow Last input Row for old extent we are adding data to
     *  @param lbid         LBID of the relevant extent.
     *  @param bIsNewExtent Treat as new or existing extent when CP min/max is
     *                      sent to BRM
     */
    virtual void addFirstEntry( RID         lastInputRow,
                                BRM::LBID_t lbid,
                                bool        bIsNewExtent );

    /** @brief Add or update an entry for the specified Row and its min/max val.
     *         If new extent, LBID will be added later when extent is allocated.
     *  @param lastInputRow Last input Row for a new extent being loaded.
     *  @param minVal       Minimum value for the latest buffer read
     *  @param maxVal       Maximum value for the latest buffer read
     */
    virtual void addOrUpdateEntry( RID     lastInputRow,
                                   int64_t minVal,
                                   int64_t maxVal,
                                   ColDataType colDataType );

    /** @brief Send updated Casual Partition (CP) info to BRM.
     */
    virtual void getCPInfoForBRM ( JobColumn column,
                                   BRMReporter& brmReporter );

    /** @brief Debug print function.
     */
    virtual void print( const JobColumn& column );

    /** @brief Add extent's LBID to the oldest entry that is awaiting an LBID
     *  @param startLbid Starting LBID for a pending extent.
     *  @return NO_ERROR upon success; else error if extent entry not found
     */
    virtual int updateEntryLbid( BRM::LBID_t startLbid );

private:
    OID             fColOid;              // Column OID for the relevant extents
    Log*            fLog;                 // Log used for debug logging
    boost::mutex    fMapMutex;            // protects unordered map access
    std::set<RID>   fPendingExtentRows;   // list of lastInputRow entries that
                                          // are awaiting an LBID assignment.

    // unordered map where we collect the min/max values per extent
    std::tr1::unordered_map<RID,ColExtInfEntry,uint64Hasher> fMap;

    // disable copy constructor and assignment operator
    ColExtInf(const ColExtInf&);
    ColExtInf& operator=(const ColExtInf&);
};

} //end of namespace

#endif // WE_COLEXTINF_H_
