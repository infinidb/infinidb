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
#include <stdint.h>
#include <string>
#include <sstream>
#include <iomanip>
#include <cstdlib>
#ifndef _MSC_VER
#include <arpa/inet.h>
#else
#include <ws2tcpip.h>
#endif
#include <cstring>
using namespace std;

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
namespace bu=boost::uuids;

#include "idbu16b.h"

namespace
{

uint64_t swap64(uint64_t in)
{
	uint64_t ret=0;
	ret = ntohl(in & 0xffffffff);
	ret <<= 32;
	ret |= ntohl((in >> 32) & 0xffffffff);
	return ret;
}

}

namespace utils {

idbu16b& idbu16b::operator+=(const idbu16b& rhs)
{
	uint64_t tlo = lo;
	tlo += rhs.lo;
	if (tlo < lo || tlo < rhs.lo)
	{
		hi++;
	}
	lo = tlo;
	hi += rhs.hi;
	return *this;
}

idbu16b& idbu16b::operator-=(const idbu16b& rhs)
{
	uint64_t tlo = lo;
	tlo -= rhs.lo;
	if (tlo > lo || tlo > rhs.lo)
	{
		--hi;
	}
	lo = tlo;
	hi -= rhs.hi;
	return *this;
}

string idbu16b::toUUIDString() const
{
	bu::uuid u;
	uint64_t work;

	work = swap64(hi);
	memcpy(&u.data[0], &work, 8);
	work = swap64(lo);
	memcpy(&u.data[8], &work, 8);
	return bu::to_string(u);
}

string idbu16b::toIPv6String() const
{
	string ret;
	char buf[INET6_ADDRSTRLEN];
	const char* p;
	struct in6_addr addr;
	char* a;
	a = reinterpret_cast<char*>(&addr);
	memcpy(a, &lo, 8);
	memcpy(a+8, &hi, 8);
	p = inet_ntop(AF_INET6, &addr, buf, INET6_ADDRSTRLEN);
	if (p)
	{
		ret = p;
	}
	return ret;
}

/*static*/
idbu16b idbu16b::fromUUIDString(const string& in)
{
	idbu16b ret;
	bu::uuid u;
	bu::string_generator gen;
	u = gen(in);
	uint64_t* p=0;
	p = reinterpret_cast<uint64_t*>(&u.data[0]);
	ret.hi = swap64(*p);
	p = reinterpret_cast<uint64_t*>(&u.data[8]);
	ret.lo = swap64(*p);
	return ret;
}

/*static*/
idbu16b idbu16b::fromIPv6String(const string& in)
{
	idbu16b ret;
	ret.markNull();
	int rc;
	struct in6_addr addr;
	memset(&addr, 0, sizeof(addr));
	rc = inet_pton(AF_INET6, in.c_str(), &addr);
	if (rc == 1)
	{
		uint64_t* p=0;
		p = reinterpret_cast<uint64_t*>(&addr);
		ret.lo = *p;
		p++;
		ret.hi = *p;
	}
	return ret;
}

string idbu16b::toString() const
{
	ostringstream oss;
	oss << hex << setfill('0');
	oss << setw(16) << hi << ':' << setw(16) << lo;
	return oss.str();
}

}

