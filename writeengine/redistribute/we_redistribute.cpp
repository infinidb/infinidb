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

/*
* $Id: we_redistribute.cpp 4216 2012-09-28 23:08:21Z xlou $
*/


#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "we_redistributedef.h"
#include "we_redistributecontrol.h"
#include "we_redistribute.h"

namespace redistribute
{

void Redistribute::handleRedistributeMessage(ByteStream& bs, IOSocket& ios)
{
	// consume the WES message id
	ByteStream::byte wesMsgId;
	bs >> wesMsgId;

	// peek at the message header
	const RedistributeMsgHeader* h = (const RedistributeMsgHeader*) bs.buf();
	switch (h->messageId)
	{
		case RED_CNTL_START:
		case RED_CNTL_STATUS:
		case RED_CNTL_STOP:
		case RED_CNTL_CLEAR:
		case RED_CNTL_RESP:
			RedistributeControl::instance()->handleUIMsg(bs, ios);
			break;

		default:
			RedistributeControl::instance()->handleJobMsg(bs, ios);
			break;
	}
}


}



// vim:ts=4 sw=4:

