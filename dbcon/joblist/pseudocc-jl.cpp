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


#include "pseudocc-jl.h"
#include "pseudocolumn.h"
#include "nullvaluemanip.h"

using namespace std;
using namespace messageqcpp;
using namespace execplan;

namespace joblist {

PseudoCCJL::PseudoCCJL(const PseudoColStep &pcs) : ColumnCommandJL(pcs), function(pcs.pseudoColumnId())
{
}

PseudoCCJL::~PseudoCCJL()
{
}

void PseudoCCJL::createCommand(ByteStream &bs) const
{
	bs << (uint8_t) PSEUDO_COLUMN;
	bs << function;
	ColumnCommandJL::createCommand(bs);
}

void PseudoCCJL::runCommand(ByteStream &bs) const
{
	if (function == PSEUDO_EXTENTMAX) {
		int64_t max = extents[currentExtentIndex].partition.cprange.hi_val;
		int64_t min = extents[currentExtentIndex].partition.cprange.lo_val;
		if (extents[currentExtentIndex].partition.cprange.isValid == BRM::CP_VALID && max >= min)
			bs << max;
		else
			bs << utils::getNullValue(colType.colDataType, colType.colWidth);
	}
	else if (function == PSEUDO_EXTENTMIN) {
		int64_t max = extents[currentExtentIndex].partition.cprange.hi_val;
		int64_t min = extents[currentExtentIndex].partition.cprange.lo_val;
		if (extents[currentExtentIndex].partition.cprange.isValid == BRM::CP_VALID && max >= min)
			bs << min;
		else
			bs << utils::getNullValue(colType.colDataType, colType.colWidth);
	}
	else if (function == PSEUDO_EXTENTID)
		bs << extents[currentExtentIndex].range.start;
	ColumnCommandJL::runCommand(bs);
}

string PseudoCCJL::toString()
{
	ostringstream oss;
	oss << "PseudoColumnJL fcn: " << function << " on: " << ColumnCommandJL::toString();
	return oss.str();
}





}
