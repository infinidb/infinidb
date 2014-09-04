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

/***********************************************************************
*   $Id: socket.h 3048 2012-04-04 15:33:45Z rdempsey $
*
*
***********************************************************************/
/** @file */
#ifndef MESSAGEQCPP_SOCKET_H
#define MESSAGEQCPP_SOCKET_H

#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <boost/shared_ptr.hpp>

class MessageQTestSuite;

namespace messageqcpp { 
class ByteStream;
class IOSocket;
class SocketParms;

typedef boost::shared_ptr<ByteStream> SBS;

/** an abstract socket class interface
 *
 */
class Socket
{
public:
	/** dtor
	 *
	 */
	virtual ~Socket() {}

	/** open the socket
	 *
	 */
	virtual void open() = 0;

	/** read a message from the socket
	 * 
	 * wait for and return a message from the socket. The deafult timeout waits forever. Note that
	 * eventhough struct timespec has nanosecond resolution, this method only has milisecond resolution.
	 */
	virtual const SBS read(const struct timespec* timeout=0, bool* isTimeOut = NULL) const = 0;

	/** write a message to the socket
	 * 
	 * write a message to the socket
	 */
	virtual void write(const ByteStream& msg) const = 0;
	virtual void write_raw(const ByteStream& msg) const = 0;

	/** close the socket
	 *
	 */
	virtual void close() = 0;

	/** bind to a port
	 *
	 */
	virtual void bind(const struct sockaddr* serv_addr) = 0;

	/** listen for connections
	 *
	 */
	virtual void listen(int backlog=5) = 0;

	/** return an (accepted) IOSocket ready for I/O
	 *
	 */
	virtual const IOSocket accept(const struct timespec* timeout=0) = 0;

	/** connect to a server socket
	 *
	 */
	virtual void connect(const sockaddr* serv_addr) = 0;

	/** test if this socket is open
	 *
	 */
	virtual const bool isOpen() const = 0;

	/** get the SocketParms
	 *
	 */
	virtual const SocketParms socketParms() const = 0;

	/** set the SocketParms
	 *
	 */
	virtual void socketParms(const SocketParms& socketParms) = 0;

	/** set the sockaddr struct
	 *
	 */
	virtual void sa(const sockaddr* sa) = 0;

	/** dynamically allocate a copy of this object
	 *
	 */
	virtual Socket* clone() const = 0;

	/** set the connection timeout (in ms)
	 *
	 */
	virtual void connectionTimeout(const struct ::timespec* timeout) = 0;

	/** set the connection protocol to be synchronous
	 *
	 */
	virtual void syncProto(bool use) = 0;

	virtual const int getConnectionNum() const = 0;

	/** return the address as a string
	 *
	 */
	virtual const std::string addr2String() const = 0;

	/** compare 2 addresses
	 *
	 */
	virtual const bool isSameAddr(const Socket* rhs) const = 0;

	/*
	 * allow test suite access to private data for OOB test
	 */
	friend class ::MessageQTestSuite;

};

} //namespace messageqcpp

#endif //MESSAGEQCPP_SOCKET_H

