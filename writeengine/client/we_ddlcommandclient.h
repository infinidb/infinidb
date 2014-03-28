/* Copyright (C) 2013 Calpont Corp.

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

#ifndef WE_DDLCOMMANDCLIENT_H__
#define WE_DDLCOMMANDCLIENT_H__

#include <unistd.h>

#include "bytestream.h"

#include "we_messages.h"
#include "we_clients.h"
#include "dbrm.h"
#include "liboamcpp.h"
#include "writeengine.h"

#if defined(_MSC_VER) && defined(xxxWE_DDLCOMMANDCLIENT_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

#include <boost/date_time/gregorian/gregorian.hpp>
#include "dataconvert.h"

namespace WriteEngine
{
class WE_DDLCommandClient
{
	public:
		EXPORT WE_DDLCommandClient();
		EXPORT ~WE_DDLCommandClient();
		/** @brief Update SYSCOLUMN nextval column for the columnoid with nextVal.
		*
		* Update SYSCOLUMN nextval column for the columnoid with nexValue.
		* @param columnOid (in) The column OID
		* @param nextVal (in) The partition number
		* @return 0 on success, non-0 on error.
		*/		
		EXPORT uint8_t UpdateSyscolumnNextval(uint32_t columnOid, uint64_t nextVal, uint32_t sessionID = 0);
		
	private:
		BRM::DBRM fDbrm;
		WEClients* fWEClient;
		oam::Oam fOam;
	
};

}

#undef EXPORT

#endif
