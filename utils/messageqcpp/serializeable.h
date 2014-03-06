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
 * $Id$
 *
 *****************************************************************************/

/** @file 
 * class XXX interface
 */

#ifndef _SERIALIZEABLE_H_
#define _SERIALIZEABLE_H_

namespace messageqcpp {

class ByteStream;

/** This is an abstract class that defines the interface ByteStream will
   use to serialize and deserialize your class. 

	To serialize an object, do 'ByteStream << object'
	To deserialize an object, instantiate one of its type and do 'ByteStream >> object'
*/

class Serializeable {
public:
	/** dtor
	 *
	 */
	virtual ~Serializeable() { };
	/** serialize interface
	 *
	 */
	virtual void serialize(ByteStream&) const = 0;
	/** deserialize interface
	 *
	 */
	virtual void deserialize(ByteStream&) = 0;
};

}

#endif
