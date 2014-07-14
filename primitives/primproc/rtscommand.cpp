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
// $Id: rtscommand.cpp 2035 2013-01-21 14:12:19Z rdempsey $
// C++ Implementation: rtscommand
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
#include "exceptclasses.h"

using namespace std;
using namespace messageqcpp;
using namespace rowgroup;
using namespace logging;

namespace primitiveprocessor
{

RTSCommand::RTSCommand() : Command(RID_TO_STRING), absNull(false)
{
}

RTSCommand::~RTSCommand()
{
}

void RTSCommand::execute()
{
	throw logic_error("RTSCommand shouldn't be used for filter steps");
}

void RTSCommand::project()
{
	uint32_t i;

	if (passThru) {
// 		if (bpp->absRids.get() == NULL)
		if (absNull)   //@bug 1003. Always need to remake absRids
		{
			bpp->absRids.reset(new uint64_t[LOGICAL_BLOCK_RIDS]);
			for (i = 0; i < bpp->ridCount; i++)
				bpp->absRids[i] = bpp->relRids[i] + bpp->baseRid;
		}
		// need something in values

		dict.project();
	}
	else {
		int64_t tmpValues[LOGICAL_BLOCK_RIDS];
		uint32_t old_rc = bpp->ridCount;
		if (bpp->absRids.get() == NULL)
			bpp->absRids.reset(new uint64_t[LOGICAL_BLOCK_RIDS]);

		col.execute(tmpValues);

		if (old_rc != bpp->ridCount) {
			ostringstream os;

			os << __FILE__ << " (token column) error on projection for oid " << col.getOID() << " lbid " << col.getLBID();
			os << ": input rids " << old_rc;
			os << ", output rids " << bpp->ridCount << endl;
			if (bpp->sessionID & 0x80000000)
				throw NeedToRestartJob(os.str());
			else {
				throw PrimitiveColumnProjectResultExcept(os.str());
			}
		}

		dict.project(tmpValues);
	}
}

void RTSCommand::projectIntoRowGroup(RowGroup &rg, uint32_t colNum)
{
	uint32_t i;

	if (passThru) {
		if (absNull)   //@bug 1003. Always need to remake absRids
		{
			bpp->absRids.reset(new uint64_t[LOGICAL_BLOCK_RIDS]);
			for (i = 0; i < bpp->ridCount; i++)
				bpp->absRids[i] = bpp->relRids[i] + bpp->baseRid;
		}
		dict.projectIntoRowGroup(rg, colNum);
	}
	else {
		int64_t tmpValues[LOGICAL_BLOCK_RIDS];
		uint32_t old_rc = bpp->ridCount;
		if (bpp->absRids.get() == NULL)
			bpp->absRids.reset(new uint64_t[LOGICAL_BLOCK_RIDS]);

		col.execute(tmpValues);

		if (old_rc != bpp->ridCount) {

			ostringstream os;

			os << __FILE__ << " (token column) error on projection for oid " << col.getOID() << " lbid " << col.getLBID();
			os << ": input rids " << old_rc;
			os << ", output rids " << bpp->ridCount << endl;
			if (bpp->sessionID & 0x80000000)
				throw NeedToRestartJob(os.str());
			else
				throw PrimitiveColumnProjectResultExcept(os.str());
		}

		dict.projectIntoRowGroup(rg, tmpValues, colNum);
	}
}

uint64_t RTSCommand::getLBID()
{
	if (!passThru)
		return col.getLBID();
	else
		return 0;
}

void RTSCommand::nextLBID()
{
	if (!passThru)
		col.nextLBID();
}

void RTSCommand::prep(int8_t outputType, bool makeAbsRids)
{
	if (!passThru)
		col.prep(OT_BOTH, true);
	dict.prep(OT_DATAVALUE, true);
}

void RTSCommand::createCommand(ByteStream &bs)
{
	bs.advance(1);
	bs >> passThru;
	if (!passThru) {
		col.createCommand(bs);
	}
	dict.createCommand(bs);
	Command::createCommand(bs);
}

void RTSCommand::resetCommand(ByteStream &bs)
{
	if (!passThru)
		col.resetCommand(bs);
	dict.resetCommand(bs);
}

SCommand RTSCommand::duplicate()
{
	SCommand ret;
	RTSCommand *rts;

	ret.reset(new RTSCommand());
	rts = (RTSCommand *) ret.get();
	rts->passThru = passThru;
	if (!passThru)
		rts->col = col;
	rts->dict = dict;
	rts->Command::duplicate(this);
	return ret;
}

bool RTSCommand::operator==(const RTSCommand &r) const
{
	if (passThru != r.passThru)
		return false;
	if (!passThru)
		if (col != r.col)
			return false;
	if (absNull != r.absNull)
		return false;
	if (dict != dict)
		return false;
	return true;
}

bool RTSCommand::operator!=(const RTSCommand &r) const
{
	return !(*this == r);
}

void RTSCommand::getLBIDList(uint32_t loopCount, vector<int64_t> *lbids)
{
	dict.getLBIDList(loopCount, lbids);
	if (!passThru)
		col.getLBIDList(loopCount, lbids);
}

void RTSCommand::setBatchPrimitiveProcessor(BatchPrimitiveProcessor *b)
{
	Command::setBatchPrimitiveProcessor(b);
	dict.setBatchPrimitiveProcessor(b);
	if (!passThru)
		col.setBatchPrimitiveProcessor(b);
}

};
