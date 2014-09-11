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

/*****************************************************************************
 * $Id: aggregator.h 7409 2011-02-08 14:38:50Z rdempsey $
 *
 ****************************************************************************/

#ifndef AGGREGATOR_H
#define AGGREGATOR_H

/** @file */

#include <sstream>
#include <vector>
#include <list>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif
#include <boost/shared_ptr.hpp>

#include "elementtype.h"
#include "hasher.h"
#include "returnedcolumn.h"

namespace joblist {

/** @brief class TupleHasher
 *
 */
class TupleHasher
{
private:
    utils::Hasher fHasher;
    uint32_t fHashLen;

public:
    TupleHasher(uint32_t len) : fHashLen(len) {}
    uint32_t operator()(const char* v) const
    {
        return fHasher(v, fHashLen);
    }
};

/** @brief class TupleComparator
 *
 */
class TupleComparator
{
private:
    uint32_t fCmpLen;

public:
    TupleComparator(uint32_t len) : fCmpLen(len) {}
    bool operator()(const char* v1, const char *v2) const
    {
        return (memcmp(v1, v2, fCmpLen) == 0);
    }
};

/** @brief struct MetaData
 *
 * Describe meta data of columns out of the raw data
 */
struct MetaData
{
    execplan::CalpontSystemCatalog::ColType colType;
    uint32_t startPos; 
};
typedef std::vector<MetaData> MetaDataVec;

/** @brief struct AggHashMap
 *
 * To hold template data structures
 */ 
template <typename result_t>
struct AggHashMap
{
    typedef std::tr1::unordered_map<char*, result_t, TupleHasher, TupleComparator> TupleHashMap;
    typedef typename std::tr1::unordered_map<char*, result_t, TupleHasher, TupleComparator>::iterator TupleHMIter;
    typedef boost::shared_ptr<TupleHashMap> SHMP;	
};

/** @brief enum the bit flag to indicate a tuple qualified filter or not*/
enum KEEP_FLAG
{
    KEEP = 0,
    DROP
};

/** @brief class Aggregator
 *
 * Perform grouping and aggregate operations
 */
class Aggregator
{
public:
    
    /** @brief aggregator operators */
    enum AggOp
    {
        COUNT = 0,
        SUM,
        AVG,
        MIN,
        MAX
    };
  
    /** @brief Constructor */
    Aggregator(AggOp aggOp, 
               MetaDataVec& aggCols, 
               execplan::SRCP aggParam,
               uint32_t hashLen,
               uint64_t bucketID);
               
    /** @brief Destructor */
    virtual ~Aggregator();
    
    /** @brief aggregate main function 
     *
     * @param map template hashmap reference.
     *        vt empty vector reference of tuple
     * @output hash map with group results 
     *         populated vector of tuple
     */
    template <typename result_t>
   void doAggregate(typename AggHashMap<result_t>::SHMP& map, TupleBucketDataList *inDL, std::vector<TupleType> &vt);
    
    /** @brief aggregate main function for AVG
     *
     * @param map double result hashmap reference
     * @note because div operation is needed, this function is explicitly typed
     */    
    void doAggregate_AVG( AggHashMap<double>::SHMP& map, TupleBucketDataList *inDL, std::vector<TupleType> &vt);  
    void bucketID (uint64_t bucketID) { fBucketID = bucketID;}
    
private:
    /** @brief constructor for completeness */
    Aggregator();
    Aggregator(const Aggregator& hj);
    
    /** @brief get agg function result 
     */
    template <typename result_t>
    void getAggResult(TupleType& tt, result_t& result);
    
    /** @brief update agg function result for this group
     */
    template <typename result_t>
    void updateAggResult(TupleType& tt, result_t& result);
    
    uint8_t fAggOp;             // aggregate operator
    MetaDataVec fAggCols;       // aggregate columns meta data
    execplan::SRCP fAggParam;   // aggregate parameter
    uint64_t fHashLen;          // hash length
    uint64_t fBucketID;         // bucket ID this aggregator handles
};

} //namespace

#endif
