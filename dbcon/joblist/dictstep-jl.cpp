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

//
// $Id: dictstep-jl.cpp 9655 2013-06-25 23:08:13Z xlou $
// C++ Implementation: dictstep-js
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include "bpp-jl.h"

using namespace std;
using namespace messageqcpp;

namespace joblist {

DictStepJL::DictStepJL()
{
}

DictStepJL::DictStepJL(const pDictionaryStep &dict)
{
	BOP = dict.fBOP;
	OID = dict.oid();
	colName = dict.name();
	compressionType = dict.colType().compressionType;

	hasEqFilter = dict.hasEqualityFilter;
	if (hasEqFilter) {
		//cout << "saw eqfilter\n";
		eqOp = dict.tmpCOP;
		eqFilter = dict.eqFilter;
	}
	else
		filterString = dict.fFilterString;
	filterCount = dict.fFilterCount;
}

DictStepJL::~DictStepJL()
{
}

void DictStepJL::setLBID(uint64_t token, uint32_t dbroot)
{
// 	lbid = token >> 10;  // the lbid is calculated on the PM
}

void DictStepJL::createCommand(ByteStream &bs) const
{
	bs << (uint8_t) DICT_STEP;
	bs << BOP;
	bs << (uint8_t)compressionType;
	bs << filterCount;
	bs << (uint8_t) hasEqFilter;

	if (hasEqFilter) {
		idbassert(filterCount == eqFilter.size());
		bs << eqOp;
		for (uint32_t i = 0; i < filterCount; i++)
			bs << eqFilter[i];
	}
	else
		bs << filterString;
	CommandJL::createCommand(bs);
}

void DictStepJL::runCommand(ByteStream &bs) const
{ }

uint8_t DictStepJL::getTableColumnType()
{
	throw logic_error("Don't call DictStepJL::getTableColumn(); it's not a projection step");
}

string DictStepJL::toString()
{
	ostringstream os;

	os << "DictStepJL: " << filterCount << " filters, BOP=" << (int) BOP
		<< ", oid=" << OID << " name=" << colName << endl;
	return os.str();
}

uint16_t DictStepJL::getWidth()
{
	return colWidth;
}

void DictStepJL::setWidth(uint16_t w)
{
	colWidth = w;
}

}; // namespace
