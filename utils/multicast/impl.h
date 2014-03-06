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
 * $Id: impl.h 3495 2013-01-21 14:09:51Z rdempsey $
 *
 *****************************************************************************/

/* This code is based on udpcast-20090830. Most of the source code in that release
   contains no copyright or licensing notices at all. The exception is fec.c, which
   is not used here. The udpcast website, http://udpcast.linux.lu/, implies that
   the source is covered under GPL. */

/** @file */

#ifndef MCAST_IMPL_H__
#define MCAST_IMPL_H__

#include <string>
#include <stdint.h>

#include "bytestream.h"

#include "udpcast.h"
#include "participants.h"

namespace multicast
{

class MulticastImpl
{
public:
	MulticastImpl(int min_receivers, const std::string& ifName, int portBase=9000, int bufSize=8*1024*1024);
	~MulticastImpl();

	void startSender();
	void doTransfer(const uint8_t* buf, uint32_t len);

	void startReceiver();
	void receive(messageqcpp::SBS obs);

	struct net_config fNet_config;
	struct stat_config fStat_config;
	struct client_config fClient_config;
	std::string fIfName;
	int fSock[3];
	participantsDb_t fDb;
};

}

#endif

