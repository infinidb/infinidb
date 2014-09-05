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

/*******************************************************************************
* $Id: we_message_handlers.h 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
#ifndef WE_MESSAGE_HANDLERS_H__
#define WE_MESSAGE_HANDLERS_H__

#include "bytestream.h"

namespace WriteEngine
{

extern messageqcpp::ByteStream::byte doMsg1(messageqcpp::ByteStream& bs, std::string err);
extern messageqcpp::ByteStream::byte doMsg2(messageqcpp::ByteStream& bs, std::string err);
extern messageqcpp::ByteStream::byte doUpdateSyscolumnNextval(messageqcpp::ByteStream& bs, std::string err);
}

#endif

