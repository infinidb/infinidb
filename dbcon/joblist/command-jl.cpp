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
// $Id: command-jl.cpp 8272 2012-01-19 16:28:34Z xlou $
// C++ Implementation: command
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

using namespace messageqcpp;

namespace joblist
{

CommandJL::CommandJL() : bpp(NULL), OID(0), tupleKey(0xFFFFFFFF) { }

CommandJL::CommandJL(const CommandJL &c) :
	bpp(c.bpp), OID(c.OID), tupleKey(c.tupleKey), colName(c.colName) { };

CommandJL::~CommandJL() { };

void CommandJL::createCommand(ByteStream &bs) const
{
	bs << OID;
	bs << tupleKey;
	// no need to send column name to PM as of rel.2.2.5.
}

void CommandJL::runCommand(ByteStream &bs) const
{ }

void CommandJL::setBatchPrimitiveProcessor(BatchPrimitiveProcessorJL *b) 
{
	bpp = b;
}

uint32_t CommandJL::getOID()
{
	return OID;
}

const std::string& CommandJL::getColName()
{
	return colName;
}

void CommandJL::setTupleKey(uint32_t tkey)
{
	tupleKey = tkey;
}

uint32_t CommandJL::getTupleKey()
{
	return tupleKey;
}

};
