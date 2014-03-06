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
*   $Id: filtercommand-jl.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/

#include "bpp-jl.h"

using namespace std;
using namespace messageqcpp;

namespace joblist
{

FilterCommandJL::FilterCommandJL(const FilterStep& step) :
	fBOP(step.BOP()), fColType(step.colType())
{
	OID = 0;
	colName = step.name();
}


FilterCommandJL::~FilterCommandJL()
{
}


void FilterCommandJL::setLBID(uint64_t rid, uint32_t dbroot)
{
}


uint8_t FilterCommandJL::getTableColumnType()
{
	throw logic_error("Don't call FilterCommandJL::getTableColumn(); it's not a projection step");
}


CommandJL::CommandType FilterCommandJL::getCommandType()
{
	return FILTER_COMMAND;
}


string FilterCommandJL::toString()
{
	ostringstream ret;

	ret << "FilterCommandJL: " << colName << " BOP=" << (uint32_t) fBOP;
	return ret.str();
}


void FilterCommandJL::createCommand(ByteStream &bs) const
{
	bs << (uint8_t) FILTER_COMMAND;
	bs << fBOP;
	CommandJL::createCommand(bs);
}


void FilterCommandJL::runCommand(ByteStream &bs) const
{
}


uint16_t FilterCommandJL::getWidth()
{
	return fColType.colWidth;
}


};
