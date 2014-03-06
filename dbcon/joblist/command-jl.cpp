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

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
namespace bu=boost::uuids;

#include "bpp-jl.h"

using namespace messageqcpp;

namespace joblist
{

CommandJL::CommandJL() :
	bpp(0),
	OID(0),
	tupleKey(0xFFFFFFFF)
{
	queryUuid = bu::nil_generator()();
	stepUuid = bu::nil_generator()();
}

CommandJL::CommandJL(const CommandJL &c) :
	bpp(c.bpp),
	OID(c.OID),
	tupleKey(c.tupleKey),
	colName(c.colName),
	queryUuid(c.queryUuid),
	stepUuid(c.stepUuid)
{
}

CommandJL::~CommandJL() { };

void CommandJL::createCommand(ByteStream &bs) const
{
	bs << OID;
	bs << tupleKey;
	bs << queryUuid;
	bs << stepUuid;
	// no need to send column name to PM as of rel.2.2.5.
}

}
