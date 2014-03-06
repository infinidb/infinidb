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
// $Id: passthrucommand.cpp 2093 2013-05-08 19:23:58Z pleblanc $
// C++ Implementation: passthrucommand
//
// Description:
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include <unistd.h>

#include "bpp.h"

using namespace std;
using namespace messageqcpp;
using namespace rowgroup;

namespace primitiveprocessor
{

PassThruCommand::PassThruCommand() : Command(PASS_THRU)
{
}

PassThruCommand::~PassThruCommand()
{
}

void PassThruCommand::prep(int8_t outputType, bool makeAbsRids)
{
	if (bpp->ot == ROW_GROUP) {
		bpp->outputRG.initRow(&r);
		rowSize = r.getSize();
	}
}

void PassThruCommand::execute()
{
// 	throw logic_error("PassThruCommand isn't a filter step");
}

void PassThruCommand::project()
{
	uint32_t i;

	*bpp->serialized << (uint32_t) (bpp->ridCount * colWidth);
#if 0
 	cout << "pass thru serializing " << (uint32_t) (bpp->ridCount * colWidth) << " bytes:\n";
 	cout << "at relative position " << bpp->serialized->length() - sizeof(ISMPacketHeader) - sizeof(PrimitiveHeader) - 4 << endl;
 	for (i = 0; i < bpp->ridCount; i++)
 		cout << "   " << i << ": " << bpp->values[i] << endl;
#endif
	switch (colWidth) {
		case 8:
			bpp->serialized->append((uint8_t *) bpp->values, bpp->ridCount << 3);
			break;
		case 4:
			for (i = 0; i < bpp->ridCount; i++)
				*bpp->serialized << (uint32_t) bpp->values[i];
			break;
		case 2:
			for (i = 0; i < bpp->ridCount; i++)
				*bpp->serialized << (uint16_t) bpp->values[i];
			break;
		case 1:
			for (i = 0; i < bpp->ridCount; i++)
				*bpp->serialized << (uint8_t) bpp->values[i];
			break;
		default:
			throw logic_error("PassThruCommand has a bad column width");
	}
}

void PassThruCommand::projectIntoRowGroup(RowGroup &rg, uint32_t col)
{
	uint32_t i;

	rg.initRow(&r);
	rg.getRow(0, &r);
	uint32_t offset = r.getOffset(col);
	rowSize = r.getSize();

	switch (colWidth) {
		case 1:
			for (i = 0; i < bpp->ridCount; i++) {
//				cout << "PTC: " << bpp->values[i] << endl;
				r.setUintField_offset<1>(bpp->values[i], offset);
				r.nextRow(rowSize);
			}
			break;
		case 2:
			for (i = 0; i < bpp->ridCount; i++) {
//				cout << "PTC: " << bpp->values[i] << endl;
				r.setUintField_offset<2>(bpp->values[i], offset);
				r.nextRow(rowSize);
			}
			break;
		case 4:
			for (i = 0; i < bpp->ridCount; i++) {
				r.setUintField_offset<4>(bpp->values[i], offset);
				r.nextRow(rowSize);
			}
			break;
		case 8:
			for (i = 0; i < bpp->ridCount; i++) {
// 				cout << "PTC: " << bpp->values[i] << endl;
				r.setUintField_offset<8>(bpp->values[i], offset);
				r.nextRow(rowSize);
			}
			break;
	}
}

uint64_t PassThruCommand::getLBID()
{
	return 0;
}

void PassThruCommand::nextLBID()
{
}

void PassThruCommand::createCommand(ByteStream &bs)
{
    bs.advance(1);
	bs >> colWidth;
	Command::createCommand(bs);
}

void PassThruCommand::resetCommand(ByteStream &bs)
{
}

SCommand PassThruCommand::duplicate()
{
	SCommand ret;
	PassThruCommand *p;

	ret.reset(new PassThruCommand());
	p = (PassThruCommand *) ret.get();
	p->colWidth = colWidth;
	p->Command::duplicate(this);
	return ret;
}

bool PassThruCommand::operator==(const PassThruCommand &p) const
{
	if (colWidth != p.colWidth)
		return false;
	return true;
}

bool PassThruCommand::operator!=(const PassThruCommand &p) const
{
	return !(*this == p);
}

};
