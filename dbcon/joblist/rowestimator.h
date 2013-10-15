/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/***********************************************************************
*   $Id: rowestimator.h 5449 2009-06-19 19:58:27Z wweeks $
*
*
***********************************************************************/
/** @file */

#ifndef JOBLIST_ROWESTIMATOR_H
#define JOBLIST_ROWESTIMATOR_H

#include <boost/shared_ptr.hpp>
#include "joblisttypes.h"
#include "calpontsystemcatalog.h"
#include "columncommand-jl.h"
#include "brmtypes.h"
#include "bytestream.h"
#include <iostream>
#include <vector>
#include "brm.h"

namespace joblist
{

/** @brief estimates row counts for a TupleBPS.
 *
 * Class RowEstimator uses Casual Partitioning information and the filter string pertaining to a particular TupleBPS object
 * to estimate cardinality.  It is used to determine which table to use as the large side table in a multijoin operation.
 */
class RowEstimator
{
public:
	/** @brief ctor
	*/
	RowEstimator(): fExtentsToSample(20), fIntDistinctAdjust(1), fDecDistinctAdjust(1), fChar1DistinctAdjust(1),
			fChar2Thru7DistinctAdjust(1), fDictDistinctAdjust(1), fDateDistinctAdjust(1) { }

	/** @brief estimate the number of rows that will be returned for a particular tuple batch primitive step.
	*
	* @param cpColVec  vector of ColumnCommandJL pointers associated to the step.
	* @param scanFlags vector of flags with one entry per extent populated by the casual
	*        partitioning evaluation, each that will be scanned are true, the ones that
	*        were eliminated by casual partitioining are false.
	* @param dbrm      DBRM object used to get the HWM.
	* @parm oid	   The objectid for the first column in the step.
	*
	*/
	uint64_t estimateRows(const std::vector<ColumnCommandJL*>& cpColVec,
			      const std::vector <bool>& scanFlags,
			      BRM::DBRM& dbrm,
			      const execplan::CalpontSystemCatalog::OID oid);

	/** @brief Estimate the number of rows that will be returned for a particular table given
	*          a ColumnCommandJL for non casual partitioning column.  Added for bug 3503.
	*
	* @param colCmd The ColumnCommandJL.
	*
	*/
	uint64_t estimateRowsForNonCPColumn(ColumnCommandJL& colCmd);

private:
	/** @brief adjusts column values so that they can be compared via ranges.
	*
	* This function provides a value for dates, datetimes, and strings that can be used for distinct value estimation and
	* comparisons.
	*
	* @param ct	The column type.
	* @param value	The column value.
	*
	*/
	uint64_t adjustValue(const execplan::CalpontSystemCatalog::ColType& ct,
        	             const uint64_t& value);

	uint daysThroughMonth(uint mth);

	uint32_t estimateDistinctValues(const execplan::CalpontSystemCatalog::ColType& ct,
                                        const uint64_t& min,
                                        const uint64_t& max,
                                        const char cpStatus);

	/** @brief returns a factor between 0 and 1 for the estimate of rows that will qualify the given individual operation.
	*
	* This function works for a single operation such as "col1 = 5".
	*
	* @param ct	 The column type.
	* @param min	 The minimum value in the range.
	* @param max	 The maximum value in the range.
	* @parm cpStatus The status of the CP data (whether it's valid).
	*
	*/
    template<class T>
	float estimateOpFactor(const T& min, const T& max, const T& value, char op, uint8_t lcf,
			       uint32_t distinctValues, char cpStatus);

	/** @brief returns a factor between 0 and 1 for the estimate of rows that will qualify
	*          the given operation(s).
	*
	* This function works for multiple operations against the same column such as
    * "col1 = 5 or col1 = 10".  It calls estimateOpFactor for each individual operation.
	*
	* @param emEntry      The extent map entry for the extent being evaluated.
	* @param msgDataPtr   The filter string.
	* @param ct	      The column type.
	* @param BOP	      The binary operator for the filter predicates (eg. OR for col1 = 5 or col1 = 10)
	* @param rowsInExtent The number of rows in the extent being evaluated.
	*
	*/
	float estimateRowReturnFactor(const BRM::EMEntry& emEntry,
                                   const messageqcpp::ByteStream* msgDataPtr,
                                   const uint16_t NOPS,
                                   const execplan::CalpontSystemCatalog::ColType& ct,
                                   const uint8_t BOP,
				   const uint32_t& rowsInExtent);

	// Configurables read from Calpont.xml - future.
	uint fExtentsToSample;
	uint fIntDistinctAdjust;
	uint fDecDistinctAdjust;
	uint fChar1DistinctAdjust;
	uint fChar2Thru7DistinctAdjust;
	uint fDictDistinctAdjust;
	uint fDateDistinctAdjust;

	static const uint32_t fRowsPerExtent = 8192 * 1024;
	static const uint32_t fBlockSize = 8192; // Block size in bytes.

	// Limits the number of comparisons for each extent.  Example, in clause w/ 1000 values will limit the checks to
	// the number below.
	static const uint32_t  fMaxComparisons = 10;

}; // RowEstimator


} // joblist
#endif
