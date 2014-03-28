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

#include <unistd.h>

#include "bytestream.h"
using namespace messageqcpp;

#include "we_messages.h"
#include "we_clients.h"
#include "resourcemanager.h"
#include "dmlpkg.h"
#include "ddlpackageprocessor.h"
#include <boost/date_time/gregorian/gregorian.hpp>
using namespace boost::gregorian;
#include "dataconvert.h"
using namespace dataconvert;
using namespace dmlpackage;


#include "we_dmlcommandclient.h"

namespace WriteEngine {
	WE_DMLCommandClient::WE_DMLCommandClient()
	{
		fWEClient = new WEClients(WEClients::DDLPROC);
	}

	WE_DMLCommandClient::~WE_DMLCommandClient()
	{
		delete fWEClient;
		fWEClient = NULL;
	}

}

