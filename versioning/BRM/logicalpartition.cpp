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

/******************************************************************************
 * $Id: logicalpartition.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 *****************************************************************************/

/** @file
 * Put LogicalPartition define here to avoid header file include confilct.
 * This struct will be used in connector, brm and writeengine
 */

#include <cstdio>
#include <sstream>
#include "bytestream.h"
#include "logicalpartition.h"

namespace BRM
{
std::string LogicalPartition::toString() const
{
	char buf[256] = {0};
	std::sprintf(buf, "%d.%d.%d", pp, seg, dbroot);
	return std::string(buf);
}

/**
 * ostream operator
 */
std::ostream& operator<<(std::ostream& output, const LogicalPartition& rhs)
{
	output << rhs.pp << "." << rhs.seg << "." << rhs.dbroot;
	return output;
}

std::istream& operator>>(std::istream& input, LogicalPartition& rhs)
{
	input >> rhs.pp;
	input.ignore();
	input >> rhs.seg;
	input.ignore();
	input >> rhs.dbroot;
	return input;
}

/**
 * bytestream operator
 */
messageqcpp::ByteStream& operator<<(messageqcpp::ByteStream &bs, const LogicalPartition& rhs)
{
	rhs.serialize(bs);
	return bs;
}

messageqcpp::ByteStream& operator>>(messageqcpp::ByteStream &bs, LogicalPartition& rhs)
{
	rhs.unserialize(bs);
	return bs;
}

}
