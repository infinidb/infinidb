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
*   $Id: iosocket.h 3014 2012-03-08 14:51:56Z rdempsey $
*
*
***********************************************************************/
/** @file */
#ifndef MESSAGEQCPP_IOSOCKET_H
#define MESSAGEQCPP_IOSOCKET_H

#include <string>
#include <cassert>
#include <sys/types.h>
#include <ctime>
#ifdef _MSC_VER
#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#else
#include <netinet/in.h>
#endif

#include "socket.h"
#include "socketparms.h"
#include "bytestream.h"

class MessageQTestSuite;

#if defined(_MSC_VER) && defined(xxxIOSOCKET_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace messageqcpp {
class ServerSocket;

/** a socket capable of I/O
 *
 */
class IOSocket
{
public:

	/** ctor
	 *
	 */
	EXPORT explicit IOSocket(Socket* socket=0);

	/** copy ctor
	 *
	 */
	EXPORT IOSocket(const IOSocket& rhs);

	/** assign op
	 *
	 */
	EXPORT IOSocket& operator=(const IOSocket& rhs);

	/** dtor
	 *
	 */
	EXPORT virtual ~IOSocket();

	/** read a ByteStream from this socket
	 *
	 * This socket needs to be connected first. Will throw runtime_error on I/O error. Caller should
	 * call close() method if exception is thrown.
	 */
	virtual const SBS read(const struct timespec* timeout=0, bool* isTimeOut = NULL) const;

	/** write a ByteStream to this socket
	 *
	 * This socket needs to be connected first. Will throw runtime_error on I/O error. Caller should
	 * call close() method if exception is thrown.
	 */
	EXPORT virtual void write(const ByteStream& msg) const;
	EXPORT virtual void write_raw(const ByteStream& msg) const;

	/** access the sockaddr member
	 */
	inline virtual const sockaddr_in& sa() const;

	/** modify the sockaddr member
	 */
	inline virtual void sa(const sockaddr_in& sa);

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
	inline virtual const SocketParms& socketParms() const;

	/** set the socket params
	 *
	 */
	inline virtual void socketParms(const SocketParms& socketParms);

	/** set the socket implementation
	 *
	 * Install a socket implementation that meets the Socket interface
	 */
	EXPORT virtual void setSocketImpl(Socket* socket);

	/** get a string rep of the IOSocket
	 *
	 */
	EXPORT virtual const std::string toString() const;

	/** syncProto() forwarder for inherited classes
	 *
	 */
	EXPORT virtual void syncProto_(bool use) { fSocket->syncProto(use); }

	EXPORT virtual int getConnectionNum() const;

	/*
	 * allow test suite access to private data for OOB test
	 */
	friend class ::MessageQTestSuite;

protected:
	/** connect() forwarder for inherited classes
	 *
	 */
	virtual void connect_(const struct sockaddr_in* serv_addr) { fSocket->connect(serv_addr); }

	/** connectionTimeout() forwarder for inherited classes
	 *
	 */
	virtual void connectionTimeout_(const struct timespec* timeout) { fSocket->connectionTimeout(timeout); }

private:
	void doCopy(const IOSocket& rhs);

	Socket* fSocket;
	sockaddr_in fSa;
};

//RJD: moving read() to lib
//inline const SBS IOSocket::read(const struct timespec* timeout, bool* isTimeOut) const { assert(fSocket); return fSocket->read(timeout, isTimeOut); }
inline const sockaddr_in& IOSocket::sa() const { return fSa; }
inline void IOSocket::sa(const sockaddr_in& sa)
	{ fSa = sa;
	if (fSocket)
		fSocket->sa( sa ); }
inline void IOSocket::open() { assert(fSocket); fSocket->open(); }
//RJD: changing close() to simply bail on null fSocket. I'm not really sure what's best here, but this is probably
//   better that asserting...
inline void IOSocket::close() { if (fSocket) fSocket->close(); }
inline bool IOSocket::isOpen() const { return (fSocket && fSocket->isOpen()); }
inline void IOSocket::write(const ByteStream& msg) const { assert(fSocket); fSocket->write(msg); }
inline void IOSocket::write_raw(const ByteStream& msg) const { assert(fSocket); fSocket->write_raw(msg); }
inline const SocketParms& IOSocket::socketParms() const { assert(fSocket); return fSocket->socketParms(); }
inline void IOSocket::socketParms(const SocketParms& socketParms) { assert(fSocket); fSocket->socketParms(socketParms); }
inline void IOSocket::setSocketImpl(Socket* socket) { delete fSocket; fSocket = socket; }
inline int IOSocket::getConnectionNum() const { return fSocket->getConnectionNum(); }

/**
 * stream an IOSocket rep to any ostream
 */
inline std::ostream& operator<<(std::ostream& os, const IOSocket& rhs)
{
	os << rhs.toString();
	return os;
}

} //namespace messageqcpp

#undef EXPORT

#endif //MESSAGEQCPP_IOSOCKET_H

