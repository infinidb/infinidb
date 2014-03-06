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
// $Id: rtscommand-jl.cpp 9655 2013-06-25 23:08:13Z xlou $
// C++ Implementation: rtscommand-jl
//
// Description:
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include "bpp-jl.h"
#include "tablecolumn.h"


using namespace std;
using namespace messageqcpp;

namespace joblist
{

RTSCommandJL::RTSCommandJL(const pColStep &c, const pDictionaryStep &d)
{

	col.reset(new ColumnCommandJL(c));
	dict.reset(new DictStepJL(d));
	/* XXXPAT: Need to validate the width; critical for tuple return functionality */
	dict->setWidth(c.realWidth);
	OID = d.oid();
	colName = d.name();
	passThru = 0;
}

RTSCommandJL::RTSCommandJL(const PassThruStep &p, const pDictionaryStep &d)
{
	execplan::CalpontSystemCatalog::ColType colType;

	dict.reset(new DictStepJL(d));
	/* XXXPAT: Need to validate the width; critical for tuple return functionality */
	dict->setWidth(p.realWidth);
	OID = d.oid();
	colName = d.name();
	passThru = 1;
}

RTSCommandJL::~RTSCommandJL()
{
}

void RTSCommandJL::setLBID(uint64_t data, uint32_t dbroot)
{
	if (!passThru)
		col->setLBID(data, dbroot);
	dict->setLBID(data, dbroot);
}

void RTSCommandJL::createCommand(ByteStream &bs) const
{
	bs << (uint8_t) RID_TO_STRING;
	bs << passThru;
	if (!passThru)
		col->createCommand(bs);
	dict->createCommand(bs);
	CommandJL::createCommand(bs);
}

void RTSCommandJL::runCommand(ByteStream &bs) const
{
	if (!passThru)
		col->runCommand(bs);
	dict->runCommand(bs);
}

uint8_t RTSCommandJL::getTableColumnType()
{
	return TableColumn::STRING;
}

string RTSCommandJL::toString()
{
	ostringstream ret;

	ret << "RTSCommandJL: oid=" << OID << " colName=" << colName << endl;
	ret << "   ";
	if (!passThru)
		ret << col->toString() << endl;
	ret << "   ";
	ret << dict->toString();
	return ret.str();
}

uint16_t RTSCommandJL::getWidth()
{
	return dict->getWidth();
}

};
