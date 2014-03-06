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
*   $Id: socketparms.cpp 3495 2013-01-21 14:09:51Z rdempsey $
*
*
***********************************************************************/

#include "socketparms.h"

namespace messageqcpp { 

SocketParms::SocketParms(int domain, int type, int protocol) :
	fSd(-1), fDomain(domain), fType(type), fProtocol(protocol)
{
}

SocketParms::~SocketParms()
{
}

void SocketParms::doCopy(const SocketParms& rhs)
{
	fSd = rhs.fSd;
	fDomain = rhs.fDomain;
	fType = rhs.fType;
	fProtocol = rhs.fProtocol;
}

SocketParms::SocketParms(const SocketParms& rhs)
{
	doCopy(rhs);
}

SocketParms& SocketParms::operator=(const SocketParms& rhs)
{
	if (this != &rhs)
	{
		doCopy(rhs);
	}

	return *this;
}

}

