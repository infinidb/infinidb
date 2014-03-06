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


/******************************************************************************
* $Id: rowestimator.cpp 5642 2009-08-10 21:04:59Z wweeks $
*
******************************************************************************/
#include <iostream>
#include "primitivemsg.h"
#include "blocksize.h"
#include "rowestimator.h"
#include "calpontsystemcatalog.h"
#include "brm.h"
#include "brmtypes.h"
#include "dataconvert.h"
#include "configcpp.h"

#define ROW_EST_DEBUG 0
#if ROW_EST_DEBUG
#include "stopwatch.h"
#endif

using namespace config;
using namespace std;
using namespace execplan;
using namespace BRM;
using namespace logging;


namespace joblist
{

// Returns the sum of the days through a particular month where 1 is Jan, 2 is Feb, ...
// This is used for converting a Calpont date to an integer representing the day number since the year 0 for use in
// calculating the number of distinct days in a range.  It doesn't account for leap years as these are rough estimates
// and only need to be accurate within an order of magnitude.
uint32_t RowEstimator::daysThroughMonth(uint32_t mth)
{
	switch(mth)
	{
		case 0:
			return 0;
		case 1:
			return 31;
		case 2:
			return 59;
		case 3:
			return 90;
		case 4:
			return 120;
		case 5:
			return 151;
		case 6:
			return 181;
		case 7:
			return 212;
		case 8:
			return 243;
		case 9:
			return 273;
		case 10:
			return 304;
		case 11:
			return 334;
		default:
			return 365;
	}
}

// This function takes a column value and if necessary adjusts it based on rules defined in the requirements.
// The values are adjusted so that one can be subtracted from another to find a range, compare, etc.
uint64_t RowEstimator::adjustValue(const execplan::CalpontSystemCatalog::ColType& ct,
							   const uint64_t& value)
{
	switch(ct.colDataType)
	{
		// Use day precision for dates.  We'll use day relative to the year 0 without worrying about leap
		// years.  This is for row count estimation and we are close enough for hand grenades.
		case CalpontSystemCatalog::DATE:
		{
			dataconvert::Date dt(value);
			return dt.year * 365 + daysThroughMonth(dt.month - 1) + dt.day;
		}

		// Use second precision for datetime estimates.  We'll use number of seconds since the year 0
		// without worrying about leap years.
		case CalpontSystemCatalog::DATETIME:
		{
			dataconvert::DateTime dtm(value);
			// 86,400 seconds in a day.
			return (dtm.year * 365 + daysThroughMonth(dtm.month - 1) + dtm.day - 1) * 86400 +
					dtm.hour * 3600 + dtm.minute * 60 + dtm.second;
		}

		// Use the first character only for estimating chars and varchar ranges.
        	// TODO:  Use dictionary column HWM for dictionary columns.
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR:
			// Last byte is the first character in the string.
			return (0xFF & value);
		default:
			return value;
	}
}

// Estimates the number of distinct values given a min/max range.  When the range has not been set,
// rules from the requirements are used based on the column type.
uint32_t RowEstimator::estimateDistinctValues(const execplan::CalpontSystemCatalog::ColType& ct,
					      const uint64_t& min,
					      const uint64_t& max,
					      const char cpStatus)
{
    uint64_t ret = 10;

    // If no casual partitioning info available for extent.  These rules were defined in the requirements.
    if(cpStatus != BRM::CP_VALID)
    {
        switch(ct.colDataType)
        {

            case CalpontSystemCatalog::BIT:
                return 2;

            // Return limit/2 for integers where limit is number of possible values.
            case CalpontSystemCatalog::TINYINT:
				return (2^8)/2;
			case CalpontSystemCatalog::UTINYINT:
				return (2^8);
            case CalpontSystemCatalog::SMALLINT:
				return (2^16)/2;
			case CalpontSystemCatalog::USMALLINT:
                return (2^16);

            // Next group all have range greater than 8M (# of rows in an extent), use 8M/2 as the estimate.
            case CalpontSystemCatalog::MEDINT:
			case CalpontSystemCatalog::UMEDINT:
			case CalpontSystemCatalog::INT:
			case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::BIGINT:
			case CalpontSystemCatalog::UBIGINT:
            case CalpontSystemCatalog::FLOAT:
			case CalpontSystemCatalog::UFLOAT:
            case CalpontSystemCatalog::DOUBLE:
			case CalpontSystemCatalog::UDOUBLE:
                return fRowsPerExtent / 2;

            // Use 1000 for dates.
            case CalpontSystemCatalog::DATE:
            case CalpontSystemCatalog::DATETIME:
                return 1000;

            // Use 10 for CHARs and VARCHARs.  We'll use 10 for whatever else.
            // Todo:  Requirement say use dictionary HWM if dictionary column, not sure that will be doable.
            default:
                return 10;
        }
    }
    else
    {
		ret = max - min + 1;
    }
    if(ret > fRowsPerExtent)
    {
		ret = fRowsPerExtent;
    }
    return ret;
}

// Returns a floating point number between 0 and 1 representing the percentage of matching rows for the given predicate against
// the given range.  This function is used for estimating an individual operation such as col1 = 2.
template<class T>
float RowEstimator::estimateOpFactor(const T& min, const T& max, const T& value, char op, uint8_t lcf, uint32_t distinctValues, char cpStatus)
{
	float factor = 1.0;
	switch(op)
	{
	       	case COMPARE_LT:
	        case COMPARE_NGE:
			if(cpStatus == BRM::CP_VALID)
			{
				factor = (1.0 * value - min) / (max - min + 1);
			}
			break;
	        case COMPARE_LE:
	        case COMPARE_NGT:
			if(cpStatus == BRM::CP_VALID)
			{
				factor = (1.0 * value - min + 1) / (max - min + 1);
           	}
			break;
	        case COMPARE_GT:
	        case COMPARE_NLE:
			if(cpStatus == BRM::CP_VALID)
			{
				factor = (1.0 * max - value) / (1.0 * max - min + 1);
			}
        		break;
	        case COMPARE_GE:
	        case COMPARE_NLT:
			if(cpStatus == BRM::CP_VALID)
			{
				// TODO:  Best way to convert to floating point arithmetic?
				factor = (1.0 * max - value + 1) / (max - min + 1);
			}
			break;
		case COMPARE_EQ:
			factor = 1.0 / distinctValues;
			break;
	        case COMPARE_NE:
			factor = 1.0 - (1.0 / distinctValues);
			break;
	}
	if(factor < 0.0)
	{
		factor = 0.0;
	}
	else if(factor > 1.0)
	{
		factor = 1.0;
	}
	return factor;
}

// Estimate the percentage of rows that will be returned for a particular extent.
// This function provides the estimate for entire filter such as "col 1 < 100 or col1 > 10000".
float RowEstimator::estimateRowReturnFactor(const BRM::EMEntry& emEntry,
                                   const messageqcpp::ByteStream* bs,
                                   const uint16_t NOPS,
                                   const execplan::CalpontSystemCatalog::ColType& ct,
                                   const uint8_t BOP,
				   const uint32_t& rowsInExtent)
{
    bool bIsUnsigned = execplan::isUnsigned(ct.colDataType);
	float factor = 1.0;
	float tempFactor = 1.0;

	// Adjust values based on column type and estimate the
	uint64_t adjustedMin = adjustValue(ct, emEntry.partition.cprange.lo_val);
	uint64_t adjustedMax = adjustValue(ct, emEntry.partition.cprange.hi_val);
    uint32_t distinctValuesEstimate = estimateDistinctValues(
				ct, adjustedMin, adjustedMax, emEntry.partition.cprange.isValid);

	// Loop through the operations and estimate the percentage of rows that will qualify.
	// For example, there are two operations for "col1 > 5 and col1 < 10":
	// 1) col1 > 5
	// 2) col2 < 10
	int length = bs->length(), pos = 0;
	const char *msgDataPtr = (const char *) bs->buf();
	int64_t value=0;
	bool firstQualifyingOrCondition = true;
	uint16_t comparisonLimit = (NOPS <= fMaxComparisons) ? NOPS : fMaxComparisons;
	for (int i = 0; i < comparisonLimit; i++) {
		pos += ct.colWidth + 2;  // predicate + op + lcf

		// TODO:  Stole this condition from lbidlist.
		// Investigate whether this can happen / should throw an error.
		if (pos > length) {
			return factor;
		}

		// Get the comparison value for the condition.
		char op = *msgDataPtr++;
		uint8_t lcf = *(uint8_t*)msgDataPtr++;
        if (bIsUnsigned)
        {
            switch (ct.colWidth)
            {
                case 1:
                {
                    uint8_t val = *(uint8_t*)msgDataPtr;
                    value = val;
                    break;
                }
                case 2:
                {
                    uint16_t val = *(uint16_t*)msgDataPtr;
                    value = val;
                    break;
                }
                case 4:
                {
                    uint32_t val = *(uint32_t*)msgDataPtr;
                    value = val;
                    break;
                }
                case 8:
                default:
                {
                    uint64_t val = *(uint64_t*)msgDataPtr;
                    value = static_cast<int64_t>(val);
                    break;
                }
            }
        }
        else
        {
            switch (ct.colWidth)
            {
                case 1:
                {
                    int8_t val = *(int8_t*)msgDataPtr;
                    value = val;
                    break;
                }
                case 2:
                {
                    int16_t val = *(int16_t*)msgDataPtr;
                    value = val;
                    break;
                }
                case 4:
                {
                    int32_t val = *(int32_t*)msgDataPtr;
                    value = val;
                    break;
                }
                case 8:
                default:
                {
                    int64_t val = *(int64_t*)msgDataPtr;
                    value = val;
                    break;
                }
            }
        }

		// TODO:  Investigate whether condition below should throw an error.
		msgDataPtr += ct.colWidth;
		if (pos > length) {
			return factor;
		}

#if ROW_EST_DEBUG
		cout << "  Min-" << emEntry.partition.cprange.lo_val <<
				", Max-" << emEntry.partition.cprange.hi_val <<
				", Val-" << value;
#endif

		// Get the factor for the individual operation.
        if (bIsUnsigned)
        {
            tempFactor = estimateOpFactor<uint64_t>(
				adjustedMin, adjustedMax, adjustValue(ct, value), op, lcf,
                distinctValuesEstimate, emEntry.partition.cprange.isValid);
        }
        else
        {
            tempFactor = estimateOpFactor<int64_t>(
				adjustedMin, adjustedMax, adjustValue(ct, value), op, lcf,
                distinctValuesEstimate, emEntry.partition.cprange.isValid);
        }

#if ROW_EST_DEBUG
		cout << ", OperatorFactor-" << tempFactor << ", DistinctValsEst-" << distinctValuesEstimate << endl;
#endif

		// Use it in the overall factor.
		if (BOP == BOP_AND) {
			// TODO:  Handle betweens correctly (same as a >= 5 and a <= 10)
			factor *= tempFactor;
		}
		else if (BOP == BOP_OR) {
			if (firstQualifyingOrCondition) {
				factor = tempFactor;
				firstQualifyingOrCondition = false;
			}
			else {
				factor += tempFactor;
			}
		}
		else {
			factor = tempFactor;
		}

	} // for()
	if(factor > 1.0)
	{
		factor = 1.0;
	}

#if ROW_EST_DEBUG
	if(NOPS > 1)
		cout << "  FilterFactor-" << factor << endl;
#endif

	return factor;
}

// This function returns the estimated row count for the entire TupleBPS.  It samples the last 20 (configurable) extents to
// calculate the estimate.
uint64_t RowEstimator::estimateRows(const vector<ColumnCommandJL*>& cpColVec,
				    const std::vector<bool>& scanFlags,
                                    BRM::DBRM& dbrm,
                                    const execplan::CalpontSystemCatalog::OID oid)

{
#if ROW_EST_DEBUG
	StopWatch stopwatch;
	stopwatch.start("estimateRows");
#endif

	uint32_t rowsInLastExtent = fRowsPerExtent;
	uint32_t extentRows = 0;
	HWM_t hwm = 0;
	float factor = 1.0;
	float tempFactor = 1.0;

	ColumnCommandJL* colCmd = 0;
	uint32_t extentsSampled = 0;
	uint64_t totalRowsToBeScanned = 0;
	uint32_t estimatedExtentRowCount = 0;
	uint64_t estimatedRowCount = 0;
	//vector<EMEntry> *extents = NULL;

	// Nothing to do if no scanFlags.
	if(scanFlags.size() == 0 || cpColVec.size() == 0)
	{
		// TODO:  Probably should throw an error here.
		return 0;
	}

	// Use the HWM for the estimated row count in the last extent.
	colCmd = cpColVec[0];
	const vector<EMEntry> &extents = colCmd->getExtents();
	hwm = extents.back().HWM;   // extents is sorted by "global" fbo
	rowsInLastExtent = ((hwm+1) * fBlockSize/ colCmd->getColType().colWidth)%fRowsPerExtent;

	// Sum up the total number of scanned rows.
	uint32_t idx = scanFlags.size() - 1;
	bool done = false;
	while (!done)
	{
		if(scanFlags[idx])
		{
			extentRows = (idx == scanFlags.size() - 1 ? rowsInLastExtent : fRowsPerExtent);

			// Get the predicate factor.
#if ROW_EST_DEBUG
			cout << endl;
			cout << "Ext-" << idx << ", rowsToScan-" << extentRows << endl;
#endif
			factor = 1.0;
			for (uint32_t j = 0; j < cpColVec.size(); j++)
			{
				colCmd = cpColVec[j];
				//RowEstimator rowEstimator;
#if ROW_EST_DEBUG
				stopwatch.start("estimateRowReturnFactor");
#endif
				//tempFactor =  rowEstimator.estimateRowReturnFactor(
				tempFactor = estimateRowReturnFactor(
					colCmd->getExtents()[idx],
					&(colCmd->getFilterString()),
					colCmd->getFilterCount(),
					colCmd->getColType(),
					colCmd->getBOP(),
					extentRows);
#if ROW_EST_DEBUG
				stopwatch.stop("estimateRowReturnFactor");
#endif

				factor *= tempFactor;
			}
			extentsSampled++;
			totalRowsToBeScanned += extentRows;
			estimatedExtentRowCount = uint64_t(ceil(factor * extentRows));
			if(estimatedExtentRowCount <= 0) estimatedExtentRowCount = 1;
			estimatedRowCount += estimatedExtentRowCount;
#if ROW_EST_DEBUG
			cout << "ExtentFactor-" << factor << ", EstimatedRows-" << estimatedExtentRowCount << endl;
#endif
		}

		if(extentsSampled == fExtentsToSample || idx == 0)
		{
			done = true;
		}
		else
		{
			idx--;
		}
	}

	// If there are more extents than we sampled, add the row counts for the qualifying extents
	// that we didn't sample to the count of rows that will be scanned.
	if((extentsSampled >= fExtentsToSample) && (idx > 0))
	{
		factor = (1.0 * estimatedRowCount) / (1.0 * totalRowsToBeScanned);
#if ROW_EST_DEBUG
		cout << "overall factor-" << factor << endl;
#endif
		for(uint32_t i = 0; i < idx; i++)
		{
			if(scanFlags[i])
			{
				// Don't take the expense of checking to see if the last extent was one that wasn't
				// sampled.  It will more than likely have been the first extent sampled since we
				// are doing them in reverse order.  If not, the amount of rows not populated isn't
				// that significant since there are many qualifying extents.
				totalRowsToBeScanned += fRowsPerExtent;
			}
		}
		estimatedRowCount = uint64_t(ceil(factor * totalRowsToBeScanned));
	}

#if ROW_EST_DEBUG
	cout << "Oid-" << oid << ", TotalEstimatedRows-" << estimatedRowCount << endl;
	stopwatch.stop("estimateRows");
	stopwatch.finish();
#endif
	return estimatedRowCount;
}

// @Bug 3503.  Fix to use the number of extents to estimate the number of rows in queries that do
// joins on dictionaries or other column types that do not use casual partitioning.
// We use an estimate of 100% of the rows regardless of any dictionary filters.
uint64_t RowEstimator::estimateRowsForNonCPColumn(ColumnCommandJL& colCmd)
{
	uint64_t estimatedRows = 0;
	int numExtents = colCmd.getExtents().size();
	if (numExtents > 0)
	{
		HWM_t hwm = colCmd.getExtents()[numExtents - 1].HWM;
		uint32_t rowsInLastExtent =
					((hwm+1) * fBlockSize/ colCmd.getColType().colWidth)%fRowsPerExtent;
		estimatedRows = fRowsPerExtent * (numExtents - 1) + rowsInLastExtent;
	}
	return estimatedRows;
}

} //namespace joblist

