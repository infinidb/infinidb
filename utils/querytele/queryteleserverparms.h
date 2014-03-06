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

#ifndef QUERYTELESERVERPARMS_H__
#define QUERYTELESERVERPARMS_H__

#include <string>

namespace querytele
{

class QueryTeleServerParms
{
public:
	QueryTeleServerParms() : port(0) { }
	QueryTeleServerParms(const std::string h, int p) : host(h), port(p) { }
	~QueryTeleServerParms() { }

	std::string host;
	int port;

protected:

private:

};

}

#endif

