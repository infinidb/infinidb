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
 * $Id: aggregator.cpp 8436 2012-04-04 18:18:21Z rdempsey $
 *
 ****************************************************************************/
#include <stdexcept>
#include <string>
using namespace std;

#include "aggregator.h"
#include "jobstep.h"
using namespace execplan;

/** Debug macro */
#define A_DEBUG 0
#define A_DEBUG_VERBAL 0
#if A_DEBUG
#define ADEBUG std::cout
#else
#define ADEBUG if (false) std::cout
#endif

namespace joblist {

Aggregator::Aggregator(
               AggOp aggOp, 
               MetaDataVec& aggCols, 
               execplan::SRCP aggParam,
               uint32_t hashLen,
               uint64_t bucketID): 
                                  fAggOp (aggOp),
                                  fAggCols (aggCols),
                                  fAggParam (aggParam),
                                  fHashLen (hashLen),
                                  fBucketID (bucketID)
{
}

Aggregator::~Aggregator()
{
}

template <typename result_t>
void Aggregator::doAggregate(typename AggHashMap<result_t>::SHMP& shmp, TupleBucketDataList *inDL, vector<TupleType> &vt)
{        
    bool ret;
    uint64_t it = inDL->getIterator(fBucketID);
    TupleType tt, ttv;
    result_t r;
    typename AggHashMap<result_t>::TupleHMIter hashIt;
    
    vt.reserve(inDL->size(fBucketID));
    while ((ret=inDL->next(fBucketID, it, &tt)))
    {          
        ttv.first = tt.first;
        hashIt = shmp->find(tt.second);
        
        // if group not exist, insert the new group to the hashmap. allocate
        // hashstr to be used by both the tuple inserted to the vector, and 
        // the map key.
    	if (hashIt == shmp->end()) 
    	{		
		    getAggResult<result_t>(tt, r);   
		    //cout << "VALUE: " << r << endl;
		    ttv.second = new char[fHashLen+1];
            memcpy(ttv.second, tt.second, fHashLen);
            ttv.second[fHashLen] = KEEP;
            vt.push_back(ttv);	    
		    shmp->insert(std::pair<char*, result_t> (ttv.second, r));				    
        }
        
        // if group exist, update the group result and insert tuple to vector.
        // no memory allocation is taken place.
        else
        {
            updateAggResult<result_t>(tt, hashIt->second);
            ttv.second = hashIt->first;
            vt.push_back(ttv);
        }         
    }

#if A_DEBUG  
	uint64_t size = inDL->size(fBucketID);     
    vector<uint64_t>::iterator ridit;
    if (shmp->size() > 0) 
    {
        ADEBUG << "bucket " << fBucketID << " size=" << size 
               << " hashtable size= " << shmp->size() 
               << " vector size = " << vt.size() << std::endl;
    }
#if A_DEBUG_VERBAL            
    typename AggHashMap<result_t>::TupleHMIter iter = shmp->begin();
    for (; iter != shmp->end(); iter++)
    {
        ADEBUG << "group result=" << (*iter).second << endl;
    }
#endif  
#endif
}

void Aggregator::doAggregate_AVG( AggHashMap<double>::SHMP& shmp, TupleBucketDataList *inDL, vector<TupleType> &vt)
{
    bool ret;
    uint64_t it = inDL->getIterator(fBucketID);
    TupleType tt, ttv;
    double r;
    AggHashMap<double>::TupleHMIter hashIt;
    uint64_t *counter = 0;
    uint64_t ONE = 1;
    
    vt.reserve(inDL->size(fBucketID));
    while ((ret=inDL->next(fBucketID, it, &tt)))
    {          
        ttv.first = tt.first;
        hashIt = shmp->find(tt.second);
            
    	if (hashIt == shmp->end()) 
    	{		
		    getAggResult<double>(tt, r);   
		    // the last 8 bytes are for counter. the counter is initialized to 1 here.
		    ttv.second = new char[fHashLen + 1 + 8];
            memcpy(ttv.second, tt.second, fHashLen);
            ttv.second[fHashLen] = KEEP;
            memcpy(ttv.second+fHashLen+1, &ONE, 8);
            vt.push_back(ttv);	    
		    shmp->insert(std::pair<char*, double> (ttv.second, r));				    
        }
        else
        {
            updateAggResult<double>(tt, hashIt->second);
            counter = reinterpret_cast<uint64_t*>(hashIt->first+fHashLen+1);
			//RJD: This line used to be: '*counter++'. This doesn't do anything because the binding is
			// *counter; counter++. Maybe it's supposed to be '(*counter)++'? But that will definitly change the
			// answer to some queries. If it's supposed to be *counter; counter++ then this line and the
			// previous line are superfluous.
            (*counter)++;
            ttv.second = hashIt->first;
            vt.push_back(ttv);
        }         
    }
    
    // calculate average
    for (hashIt = shmp->begin(); hashIt != shmp->end(); hashIt++)
    {
        counter = reinterpret_cast<uint64_t*>(hashIt->first+fHashLen+1);
        hashIt->second = hashIt->second / *counter;
    }
}

template <typename result_t>
void Aggregator::getAggResult(TupleType& tt, result_t& result) 
{       
    if (fAggOp == COUNT)
    {
        result = 1;
        return;
    }
    execplan::CalpontSystemCatalog::ColType& colType = fAggCols[0].colType;   
    memcpy(&result, tt.second+fAggCols[0].startPos, colType.colWidth);     
} 

template <typename result_t>
void Aggregator::updateAggResult(TupleType& tt, result_t& result) 
{
    result_t val;
    switch(fAggOp)
    {
        case SUM:
        case AVG:
            getAggResult<result_t>(tt, val);
            result += val;
            return;
        case COUNT:
            result += 1;
            return;
        case MIN:
        {
            result_t r;
            getAggResult<result_t>(tt, r);
            if (r < result)
                result = r;
            return;
        }
        case MAX:
        {
            result_t r;
            getAggResult<result_t>(tt, r);
            if (r > result)
                result = r;
            return;
        }
        default:
            throw logic_error("not handled aggregate function");       
    }
}

template
void Aggregator::doAggregate<int64_t> 
    (AggHashMap<int64_t>::SHMP& shmp, TupleBucketDataList *inDL, vector<TupleType> &vt);

template
void Aggregator::doAggregate<string> 
    (AggHashMap<string>::SHMP& shmp, TupleBucketDataList *inDL, vector<TupleType> &vt);
    
template
void Aggregator::doAggregate<float> 
    (AggHashMap<float>::SHMP& shmp, TupleBucketDataList *inDL, vector<TupleType> &vt);
    
template
void Aggregator::doAggregate<double> 
    (AggHashMap<double>::SHMP& shmp, TupleBucketDataList *inDL, vector<TupleType> &vt);    
}
// vim:ts=4 sw=4:

