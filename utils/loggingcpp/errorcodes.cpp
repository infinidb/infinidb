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

/***********************************************************************
 *   $Id: errorcodes.cpp 3495 2013-01-21 14:09:51Z rdempsey $
 *
 *
 ***********************************************************************/

#include <string>
using namespace std;

#include "errorcodes.h"

namespace logging { 

  ErrorCodes::ErrorCodes(): fErrorCodes(), fPreamble("An unexpected condition within the query caused an internal processing error within InfiniDB. Please check the log files for more details. Additional Information: ")
  {
    fErrorCodes[batchPrimitiveStepErr] = "error in BatchPrimitiveStep.";
    fErrorCodes[tupleBPSErr] = "error in TupleBPS.";
    fErrorCodes[batchPrimitiveStepLargeDataListFileErr] = "error in BatchPrimitiveStep LargeDataList File handling.";
    fErrorCodes[bucketReuseStepErr] = "error in BucketReuseStep.";
    fErrorCodes[bucketReuseStepLargeDataListFileErr] = "error in bucketReuseStep LargeDataList File handling.";
    fErrorCodes[aggregateFilterStepErr] = "error in AggregateFilterStep.";
    fErrorCodes[filterStepErr] = "error in FilterStep.";
    fErrorCodes[functionStepErr] = "error in FunctionStep.";
    fErrorCodes[hashJoinStepErr] = "error in HashJoinStep.";
    fErrorCodes[hashJoinStepLargeDataListFileErr] = "error in HashJoinStep LargeDataList File handling.";
    fErrorCodes[largeHashJoinErr] = "error in LargeHashJoin.";
    fErrorCodes[largeHashJoinLargeDataListFileErr] = "error in LargeHashJoin LargeDataList File handling.";
    fErrorCodes[stringHashJoinStepErr] = "error in StringHashJoinStep.";
    fErrorCodes[stringHashJoinStepLargeDataListFileErr] = "error in StringHashJoinStep LargeDataList File handling.";
    fErrorCodes[tupleHashJoinTooBigErr] = "error in TupleHashJoin: join is too big.";
    fErrorCodes[threadResourceErr] = "error in ExeMgr: too many threads on the system.";
    fErrorCodes[pDictionaryScanErr] = "error in pDictionaryScan.";
    fErrorCodes[pDictionaryScanLargeDataListFileErr] = "error in pDictionaryScan LargeDataList File handling.";
    fErrorCodes[pIdxListErr] = "error in pIdxList.";
    fErrorCodes[pIdxWalkErr] = "error in pIdxWalk.";
    fErrorCodes[pnlJoinErr] = "error in PNLJoinErr.";
    fErrorCodes[reduceStepErr] = "error in ReduceStep.";
    fErrorCodes[reduceStepLargeDataListFileErr] = "error in ReduceStep LargeDataList File handling.";
    fErrorCodes[unionStepErr] = "error in UnionStep.";
    fErrorCodes[unionStepLargeDataListFileErr] = "error in UnionStep LargeDataList File handling.";
    fErrorCodes[unionStepTooBigErr] = "the union required too much memory.";
    fErrorCodes[tupleAggregateStepErr] = "error in TupleAggregateStep.";
    fErrorCodes[tupleConstantStepErr] = "error in TupleConstantStep.";
    fErrorCodes[tupleHavingStepErr] = "error in TupleHavingStep.";
    fErrorCodes[aggregateResourceErr] = "Memory required to perform aggregation exceeds the RowAggregation/MaxMemory setting.";
    fErrorCodes[makeJobListErr] = "error in MakeJobList.";
    fErrorCodes[aggregateFuncErr] = "unsupported aggregation function.";
    fErrorCodes[aggregateDataErr] = "aggregation data overflow.";
    fErrorCodes[batchPrimitiveProcessorErr] = "error in BatchPrimitiveProcessor.";
    fErrorCodes[bppSeederErr] = "error in bppSeeder.";
    fErrorCodes[primitiveServerErr] = "error in PrimitiveServer.";
    fErrorCodes[projectResultErr] = "error in BatchPrimitiveProcessor projectResult.  Please check crit.log for more details.";
    fErrorCodes[hwmRangeSizeErr] = "error in PrimitiveServer load block with HWM.  Please check crit.log for more details.";
    fErrorCodes[formatErr] = "format mismatch.";
    fErrorCodes[dataTypeErr] = "data type unknown.";
    fErrorCodes[incompatJoinCols] = "incompatible column types specified for join condition.";
    fErrorCodes[incompatFilterCols] = "incompatible column types specified for filter condition.";
  }

  string ErrorCodes::errorString(uint16_t code) const
  {
    CodeMap::const_iterator iter;
    CodeMap::key_type key = static_cast<CodeMap::key_type>(code);
    CodeMap::mapped_type msg;

    iter = fErrorCodes.find(key);
    if (iter == fErrorCodes.end())
    {
        msg = "was an unknown internal error.";
    }
    else
    {
       msg = iter->second;
    }

    return (fPreamble + msg);
  }
} //namespace logging
// vim:ts=4 sw=4:

