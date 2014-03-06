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
*   $Id: serversocket.h 3495 2013-01-21 14:09:51Z rdempsey $
*
*
***********************************************************************/
/** @file */
#ifndef MESSAGEQCPP_SERVERSOCKET_H
#define MESSAGEQCPP_SERVERSOCKET_H

#include <sys/types.h>
#ifdef _MSC_VER
#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#else
#include <netinet/in.h>
#endif

class MessageQTestSuite;

#include "socket.h"
#include "iosocket.h"

namespace messageqcpp {
class SocketParms;

/** a class capable of acting as a server listen socket
 *
 */
class ServerSocket
{
public:

	/** ctor
	 *
	 */
	explicit ServerSocket(Socket* socket=0) : fSocket(socket) {}

	/** dtor
	 *
	 */
	virtual ~ServerSocket() { delete fSocket; }

	/** bind to an port
	 *
	 * bind this ServerSocket to the address/port specified in serv_addr
	 */
	inline virtual void bind(const struct sockaddr* serv_addr);

	/** setup to listen for incoming connections
	 *
	 */
	inline virtual void listen(int backlog=5);

	/** accept an incoming connection
	 *
	 * accepts a new incoming connection and returns an IOSocket to communicate over
	 */
	inline virtual const IOSocket accept(const struct timespec* timeout=0);

	/** open the socket
	 *
	 */
	inline virtual void open();

	/** close the socket
	 *
	 */
	inline virtual void close();

	/** test if the socket is open
	 *
	 */
	inline virtual bool isOpen() const;

	/** get the socket params
	 *
	 */
	inline virtual const SocketParms socketParms() const;

	/** set the socket params
	 *
	 */
	inline virtual void socketParms(const SocketParms& socketParms);

	/** set the socket implementation
	 *
	 * Install a socket implementation that meets the Socket interface
	 */
	inline virtual void setSocketImpl(Socket* socket);

	/** set the socket sync proto
	 *
	 */
	inline virtual void syncProto(bool use);

	/*
	 * allow test suite access to private data for OOB test
	 */
	friend class ::MessageQTestSuite;

private:
	ServerSocket(const ServerSocket& rhs);
	ServerSocket& operator=(const ServerSocket& rhs);

	Socket* fSocket;
};

inline void ServerSocket::bind(const struct sockaddr* serv_addr) { fSocket->bind(serv_addr); }
inline void ServerSocket::listen(int backlog) { fSocket->listen(backlog); }
inline const IOSocket ServerSocket::accept(const struct timespec* timeout) { return fSocket->accept(timeout); }
inline void ServerSocket::open() { fSocket->open(); }
inline void ServerSocket::close() { fSocket->close(); }
inline bool ServerSocket::isOpen() const { return fSocket->isOpen(); }
inline const SocketParms ServerSocket::socketParms() const { return fSocket->socketParms(); }
inline void ServerSocket::socketParms(const SocketParms& socketParms) { fSocket->socketParms(socketParms); }
inline void ServerSocket::setSocketImpl(Socket* socket) { delete fSocket; fSocket = socket; }
inline void ServerSocket::syncProto(bool use) { fSocket->syncProto(use); }

} //namespace messageqcpp

#endif //MESSAGEQCPP_SERVERSOCKET_H

