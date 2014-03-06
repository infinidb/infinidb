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
*   $Id: errorcodes.h 3495 2013-01-21 14:09:51Z rdempsey $
*
*
***********************************************************************/
/** @file */
#ifndef LOGGING_ERRORCODES_H
#define LOGGING_ERRORCODES_H

#include <sys/types.h>
#include <map>
#include <string>
#include <stdint.h>

namespace logging 
{ 

enum ErrorCodeValues
{
 batchPrimitiveStepErr = 1,
 tupleBPSErr,
 batchPrimitiveStepLargeDataListFileErr,
 bucketReuseStepErr,
 bucketReuseStepLargeDataListFileErr,
 aggregateFilterStepErr,
 filterStepErr,
 functionStepErr,
 hashJoinStepErr,
 hashJoinStepLargeDataListFileErr,
 largeHashJoinErr,
 largeHashJoinLargeDataListFileErr,
 stringHashJoinStepErr,
 stringHashJoinStepLargeDataListFileErr,
 tupleHashJoinTooBigErr,
 threadResourceErr,
 pDictionaryScanErr,
 pDictionaryScanLargeDataListFileErr,
 pIdxListErr,
 pIdxWalkErr,
 pnlJoinErr,
 reduceStepErr,
 reduceStepLargeDataListFileErr,
 unionStepErr,
 unionStepLargeDataListFileErr,
 unionStepTooBigErr,
 tupleAggregateStepErr,
 tupleConstantStepErr,
 tupleHavingStepErr,
 makeJobListErr,
 aggregateFuncErr,
 aggregateDataErr,
//don't use 100, same as SQL_NOT_FOUND
 batchPrimitiveProcessorErr = 101,
 bppSeederErr,
 primitiveServerErr,
 projectResultErr,
 hwmRangeSizeErr,
 // user input data error
 formatErr = 201,
 dataTypeErr,
 incompatJoinCols,
 incompatFilterCols,
 aggregateResourceErr
};

struct ErrorCodes
{
	ErrorCodes();
	std::string errorString(uint16_t code) const;
private:
  	typedef std::map<ErrorCodeValues, std::string> CodeMap;

	//defaults okay
	//ErrorCodes(const ErrorCodes& rhs);
	//ErrorCodes& operator=(const ErrorCodes& rhs);

	CodeMap fErrorCodes;
	const std::string fPreamble;
};

}
#endif //LOGGING_ERRORCODES_H

