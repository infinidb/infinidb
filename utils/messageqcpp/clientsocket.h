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
*   $Id: clientsocket.h 3048 2012-04-04 15:33:45Z rdempsey $
*
*
***********************************************************************/
/** @file */
#ifndef MESSAGEQCPP_CLIENTSOCKET_H
#define MESSAGEQCPP_CLIENTSOCKET_H

#include <ctime>
#ifdef _MSC_VER
#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#else
#include <netinet/in.h>
#endif

#include "iosocket.h"

namespace messageqcpp {

/** an IOSocket that can be connected to a server
 *
 * @note this class doesn't seem to do anything useful...all the public methods call the base class
 *       implementations.
 */
class ClientSocket : public IOSocket
{
public:

	/** ctor
	 *
	 */
	explicit ClientSocket(Socket* socket=0) : IOSocket(socket) {}

	/** dtor
	 *
	 */
	virtual ~ClientSocket() {}

	/** connect this IOSocket to a server
	 *
	 * connect this IOSocket to the server specified in serv_addr. Will throw a runtime_exception
	 * if the connection fails.
	 */
	inline virtual void connect(const struct sockaddr* serv_addr);

	/** set the connection timeout on a connect() call for this socket.
	 *
	 * set the connection timeout on a connect() call for this socket.
	 */
	inline virtual void connectionTimeout(const struct timespec* timeout);

	/** set the connection protocol to be synchronous
	 *
	 * set the connection protocol on a connect() call for this socket.
	 */
	inline virtual void syncProto(bool use);

	/**
	 * @brief return the address as a string
	 */
	inline virtual const std::string addr2String() const;

	/**
	 * @brief compare the addresses of 2 ClientSocket
	 */
	inline virtual const bool isSameAddr(const ClientSocket& rhs) const;

private:
	typedef IOSocket base;

	ClientSocket(const ClientSocket& rhs);
	ClientSocket& operator=(const ClientSocket& rhs);
};

inline void ClientSocket::connect(const struct sockaddr* serv_addr) { base::connect(serv_addr); }
inline void ClientSocket::connectionTimeout(const struct timespec* timeout) { base::connectionTimeout(timeout); }
inline void ClientSocket::syncProto(bool use) { base::syncProto(use); }
inline const std::string ClientSocket::addr2String() const { return base::addr2String(); }
inline const bool ClientSocket::isSameAddr(const ClientSocket& rhs) const { return base::isSameAddr(&rhs); }

} //namespace messageqcpp

#endif //MESSAGEQCPP_CLIENTSOCKET_H

