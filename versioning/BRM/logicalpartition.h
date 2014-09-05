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
 * $Id: logicalpartition.h 1823 2013-01-21 14:13:09Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * Put LogicalPartition define here to avoid header file include confilct.
 * This struct will be used in connector, brm and writeengine
 */

#ifndef LOGICALPARTITION_H_
#define LOGICALPARTITION_H_

#include "bytestream.h"

namespace BRM 
{
// Logical partition number descriptor
struct LogicalPartition
{
	uint16_t dbroot;  // dbroot #
	uint32_t pp;      // physical partition #
	uint16_t seg;     // segment #

	LogicalPartition() : dbroot ((uint16_t)-1),
	                     pp ((uint32_t)-1),
	                     seg ((uint16_t)-1) {}

	LogicalPartition(uint16_t d, uint32_t p, uint16_t s) : dbroot(d),
	                                                       pp(p),
	                                                       seg(s)
	{}
	
	bool operator<( const LogicalPartition &n ) const
	{
		return ((pp < n.pp) ||
		        (pp == n.pp && seg < n.seg) ||
		        (pp == n.pp && seg == n.seg && dbroot < n.dbroot));
	}

	void serialize(messageqcpp::ByteStream& b) const
	{
		b << (uint16_t)dbroot;
		b << (uint32_t)pp;
		b << (uint16_t)seg;
	}

	void unserialize(messageqcpp::ByteStream& b)
	{
		b >> (uint16_t&)dbroot;
		b >> (uint32_t&)pp;
		b >> (uint16_t&)seg;
	}
	
	/** @bug4816. For output to user purpose */
	std::string toString() const;
};

/**
 * ostream operator
 */
std::ostream& operator<<(std::ostream& output, const LogicalPartition& rhs);
std::istream& operator>>(std::istream& input, LogicalPartition& rhs);

/**
 * bytestream operator
 */
messageqcpp::ByteStream& operator<<(messageqcpp::ByteStream &bs, const LogicalPartition& rhs);

messageqcpp::ByteStream& operator>>(messageqcpp::ByteStream &bs, LogicalPartition& rhs);

}
#endif
