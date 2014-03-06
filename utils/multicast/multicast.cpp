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
 * $Id: multicast.cpp 3495 2013-01-21 14:09:51Z rdempsey $
 *
 *****************************************************************************/

#include <string>
#include <stdint.h>
#include <iostream>
#include <stdexcept>
using namespace std;

#include "bytestream.h"
using namespace messageqcpp;

#include "configcpp.h"
using namespace config;

#include "multicast.h"

namespace multicast
{

Multicast::Multicast() :
	fPMCount(1),
	fIFName("eth0"),
	fPortBase(9000),
	fBufSize(8 * 1024 * 1024)
{
	int tmp;
	string stmp;

	Config* cf = Config::makeConfig();

	tmp = Config::fromText(cf->getConfig("PrimitiveServers", "Count"));
	if (tmp > 0) fPMCount = tmp;

	stmp = cf->getConfig("Multicast", "Interface");
	if (!stmp.empty()) fIFName = stmp;

	tmp = Config::fromText(cf->getConfig("Multicast", "PortBase"));
	if (tmp > 0) fPortBase = tmp;

	tmp = Config::fromText(cf->getConfig("Multicast", "BufSize"));
	if (tmp > 0) fBufSize = tmp;
}

MulticastReceiver::MulticastReceiver() :
	fPimpl(0)
{
}

MulticastReceiver::~MulticastReceiver()
{
}

SBS MulticastReceiver::receive()
{
	throw runtime_error("Multicast is not available");
	return fByteStream;
}

MulticastSender::MulticastSender() :
	fPimpl(0)
{
}

MulticastSender::~MulticastSender()
{
}

void MulticastSender::send(const ByteStream& msg)
{
	throw runtime_error("Multicast is not available");
}

} //namespace multicast

//vim:ts=4 sw=4:
