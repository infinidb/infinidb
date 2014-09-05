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
*   $Id: iosocket.cpp 3632 2013-03-13 18:08:46Z pleblanc $
*
*
***********************************************************************/
#include <string>
#include <sstream>
#include <stdexcept>
#include <cstring>
using namespace std;

#include <sys/types.h>
#ifdef _MSC_VER
#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#else
#include <arpa/inet.h>
#include <sys/socket.h>
#endif

#define IOSOCKET_DLLEXPORT
#include "iosocket.h"
#undef IOSOCKET_DLLEXPORT

namespace messageqcpp {

IOSocket::IOSocket(Socket* socket) :
	fSocket(socket), sockID(0)
{
	memset(&fSa, 0, sizeof(fSa));
}

IOSocket::~IOSocket()
{
	delete fSocket;
}

void IOSocket::doCopy(const IOSocket& rhs)
{
	fSocket = rhs.fSocket->clone();
	fSa = rhs.fSa;
	sockID = rhs.sockID;
}

IOSocket::IOSocket(const IOSocket& rhs)
{
	doCopy(rhs);
}

IOSocket& IOSocket::operator=(const IOSocket& rhs)
{
	if (this != &rhs)
	{
		delete fSocket;
		doCopy(rhs);
	}

	return *this;
}

const string IOSocket::toString() const
{
#ifdef NOSSTREAM
	return "IOSocket";
#else
	ostringstream oss;
	char buf[INET_ADDRSTRLEN];
	SocketParms sp = fSocket->socketParms();
	const sockaddr_in* sinp = reinterpret_cast<const sockaddr_in*>(&fSa);
	oss << "IOSocket: sd: " << sp.sd() <<
#ifndef _MSC_VER
	       " inet: " << inet_ntop(AF_INET, &sinp->sin_addr, buf, INET_ADDRSTRLEN) <<
#endif
	       " port: " << ntohs(sinp->sin_port);
	return oss.str();
#endif
}

} //namespace messageqcpp

