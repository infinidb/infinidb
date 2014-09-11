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
*   $Id: clientsocket.h 2394 2011-02-08 14:36:22Z rdempsey $
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
	inline virtual void connect(const struct sockaddr_in* serv_addr);

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

private:
	ClientSocket(const ClientSocket& rhs);
	ClientSocket& operator=(const ClientSocket& rhs);
};

inline void ClientSocket::connect(const struct sockaddr_in* serv_addr) { connect_(serv_addr); }
inline void ClientSocket::connectionTimeout(const struct timespec* timeout) { connectionTimeout_(timeout); }
inline void ClientSocket::syncProto(bool use) { syncProto_(use); }

} //namespace messageqcpp

#endif //MESSAGEQCPP_CLIENTSOCKET_H

