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

/** @file */

#include <exception>
#include <string>

#ifndef SOCKETCLOSED_H_
#define SOCKETCLOSED_H_

namespace messageqcpp {

/** @brief A closed socket exception class
*
*  Some sort of activity has been requested on a closed socket
*/
class SocketClosed : public std::exception 
{
	std::string _M_msg;

public:
	/** Takes a character string describing the error.  */
	explicit 
	SocketClosed(const std::string&  __arg) : _M_msg(__arg) { }

	virtual 
	~SocketClosed() throw() { }

	/** Returns a C-style character string describing the general cause of
	*  the current error (the same string passed to the ctor).  */
	virtual const char* 
	what() const throw() { return _M_msg.c_str(); }
};

}

#endif
