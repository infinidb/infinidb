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

#include <unistd.h>

#include "bytestream.h"
using namespace messageqcpp;

#include "we_messages.h"
#include "we_clients.h"
#include "resourcemanager.h"
#include "ddlpkg.h"
#include "ddlpackageprocessor.h"
#include <boost/date_time/gregorian/gregorian.hpp>
using namespace boost::gregorian;
#include "dataconvert.h"
using namespace dataconvert;
using namespace ddlpackage;
using namespace ddlpackageprocessor;

#include "we_ddlcommandclient.h"

namespace WriteEngine {
	WE_DDLCommandClient::WE_DDLCommandClient()
	{
		fWEClient = WEClients::instance(WEClients::DDLPROC);
	}

	WE_DDLCommandClient::~WE_DDLCommandClient()
	{
	
	}

	uint8_t WE_DDLCommandClient::UpdateSyscolumnNextval(uint32_t columnOid, uint64_t nextVal, uint32_t sessionID) 
	{
		ByteStream command, response;
		uint8_t err = 0;
		uint64_t uniqueId = fDbrm.getUnique64();
		fWEClient->addQueue(uniqueId);
		command << (ByteStream::byte)WE_UPDATE_NEXTVAL;
		command << uniqueId;
		command << columnOid;
		command << nextVal;
		command << sessionID;
		uint16_t dbRoot;	
		BRM::OID_t oid = 1021;
		fDbrm.getSysCatDBRoot(oid, dbRoot); 
		int pmNum = 1; 
		boost::shared_ptr<messageqcpp::ByteStream> bsIn;
		
		try {
			fOam.getDbrootPmConfig (dbRoot, pmNum);
			fWEClient->write(command, pmNum);
			while (1)
			{			
				bsIn.reset(new ByteStream());
				fWEClient->read(uniqueId, bsIn);
				if ( bsIn->length() == 0 ) //read error
				{
					err = 1;
					
					break;
				}			
				else {
					*bsIn >> err;
					break;
				}
			}
		}
		catch (...)
		{
			err = 1;
		}
		fWEClient->removeQueue(uniqueId);
		return err;
	}

}

