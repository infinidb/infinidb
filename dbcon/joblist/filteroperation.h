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
*   $Id: filteroperation.h 8436 2012-04-04 18:18:21Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef _FILTEROPERATION_H_
#define _FILTEROPERATION_H_

#include <iostream>
#include "datalist.h"
#include "elementtype.h"
#include "primitivemsg.h"
#include <stdexcept>

namespace joblist
{

/** @brief filters a data list based on a comparison with a constant or another data list.
 *
 * Class FilterOperation is used to compare values from a DataList to values in another DataList and produce
 * a new DataList with the qualifying RIDS.
 *
 * Notes:
 *   The data lists must contain values with common RIDs unless the second data list passed in is a ConstantDataList.
 *
 *   The RIDs must be from the same table and must be in the same order.  The input data lists do not have to have the
 *   same number of rows, but they must be in RID order.
 *
 *   The original input data lists are not changed.
 *
 *   The new data list produced will be a subset of the first data list passed in containing the rows that qualified.
 *
 *   If comparing with a constant, the second data list passed in should be a ConstantDataList containing one row
 *   with the constant value.  An exception will be thrown if a ConstantDataList is passed as the first datalist.
 */

class TimeSet;
 const std::string filterCompare("Filter Op compare time: ");
 const std::string filterInsert("Filter insert time: ");
 const std::string filterFinish("Filter finish time: ");

class FilterOperation
{
public:

	// Defaults okay for constructor and destructor.
	

	/** @brief compares the first data list to the second datalist based on the operator and produces third datalist.
	* 
	* @param fe the comparison operator.
	* @param dl1 the data list containing values used for the left hand side.
	* @param dl2 the data list containing values used for the right hand side of the comparison.
	* @param dlOut the data list will be returned with a subset of dl1 containing the qualifying rows.
	*/
	template<typename element_t>
	void filter(int8_t COP, FIFO<RowWrapper<element_t> >& dl1, FIFO<RowWrapper<element_t> >& dl2,
							FIFO<RowWrapper<element_t> >& dlOut, uint64_t & resultCount, TimeSet& ts);

	template<typename element_t>
	void filter(int8_t COP, FIFO<RowWrapper<element_t> >& dl1, FIFO<RowWrapper<element_t> >& dl2,
							DataList<element_t>& dlOut, uint64_t & resultCount, TimeSet& ts);

private:
	template<typename datavalue_t>
	bool compare(const int8_t COP, const datavalue_t& v1, const datavalue_t& v2);

	// defaults okay
	//FilterOperation(const FilterOperation& rhs); 		
	//FilterOperation& operator=(const FilterOperation& rhs); 	
};

} // namespace
#endif
