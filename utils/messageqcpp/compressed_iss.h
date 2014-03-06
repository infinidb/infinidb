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
*   $Id$
*
*
***********************************************************************/
/** @file */
#ifndef MESSAGEQCPP_COMPRESSED_ISS_H
#define MESSAGEQCPP_COMPRESSED_ISS_H

#include <unistd.h>
#ifndef _MSC_VER
#include <netinet/in.h>
#endif

#include "socket.h"
#include "iosocket.h"
#include "bytestream.h"
#include "inetstreamsocket.h"
#include "idbcompress.h"

namespace messageqcpp {

class CompressedInetStreamSocket : public InetStreamSocket
{
public:
	CompressedInetStreamSocket();

	virtual Socket * clone() const;
	virtual const SBS read(const struct timespec* timeout=0, bool* isTimeOut = NULL, 
		Stats *stats = NULL) const;
	virtual void write(const ByteStream& msg, Stats *stats = NULL);
	virtual void write(SBS msg, Stats *stats = NULL);
	virtual const IOSocket accept(const struct timespec *timeout);
	virtual void connect(const sockaddr *addr);
private:
	compress::IDBCompressInterface alg;
	bool useCompression;
};

} //namespace messageqcpp

#undef EXPORT

#endif
