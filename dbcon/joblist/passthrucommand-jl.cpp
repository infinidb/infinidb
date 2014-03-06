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
// $Id: passthrucommand-jl.cpp 9655 2013-06-25 23:08:13Z xlou $
// C++ Implementation: passthrucommand-jl
//
// Description:
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include <string>
#include <sstream>
using namespace std;

#include "bytestream.h"
using namespace messageqcpp;

#include "primitivestep.h"
#include "tablecolumn.h"
#include "command-jl.h"
#include "passthrucommand-jl.h"

namespace joblist
{

/* I think this class literally does nothing */

PassThruCommandJL::PassThruCommandJL(const PassThruStep &p)
{
	OID = p.oid();
	colName = p.name();
	colWidth = p.colWidth;
// 	cout << "PassThru col width = " << (int) colWidth << " for OID " << OID << endl;
	/* Is this ever a dictionary column? */
	if (p.isDictColumn)
		tableColumnType = TableColumn::STRING;
	else
		switch (colWidth) {
			case 1: tableColumnType = TableColumn::UINT8; break;
			case 2: tableColumnType = TableColumn::UINT16; break;
			case 4: tableColumnType = TableColumn::UINT32; break;
			case 8: tableColumnType = TableColumn::UINT64; break;
			default:
				throw logic_error("PassThruCommandJL(): bad column width?");
		}
}

PassThruCommandJL::~PassThruCommandJL()
{
}

void PassThruCommandJL::setLBID(uint64_t l, uint32_t dbroot)
{
}

void PassThruCommandJL::createCommand(ByteStream &bs) const
{
	bs << (uint8_t) PASS_THRU;
	bs << colWidth;
	CommandJL::createCommand(bs);
}

void PassThruCommandJL::runCommand(ByteStream &bs) const
{
}

uint8_t PassThruCommandJL::getTableColumnType()
{
	return tableColumnType;
}

string PassThruCommandJL::toString()
{
	ostringstream oss;
	oss << "PassThruCommandJL: colwidth=" << static_cast<uint32_t>(colWidth) << " oid=" << OID
		<< " colName=" << colName;
	return oss.str();
}

uint16_t PassThruCommandJL::getWidth()
{
	return colWidth;
}

};
// vim:ts=4 sw=4:

