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

#include <stdint.h>
#include <string>
#include <iosfwd>

#include <boost/functional/hash.hpp>

#include "joblisttypes.h"

namespace utils {

class idbu16b
{
public:
	explicit idbu16b(const uint64_t h=0, const uint64_t l=0) :
		lo(l),
		hi(h)
		{ }

	bool operator<(const idbu16b& rhs) const
	{
		if (hi < rhs.hi) return true;
		if (hi == rhs.hi) return (lo < rhs.lo);
		return false;
	}
	bool operator<=(const idbu16b& rhs) const { return !(rhs < *this); }
	bool operator>(const idbu16b& rhs) const { return (rhs < *this); }
	bool operator>=(const idbu16b& rhs) const { return !(*this < rhs); }

	bool operator==(const idbu16b& rhs) const { return ( hi == rhs.hi && lo == rhs.lo ); }
	bool operator!=(const idbu16b& rhs) const { return !(*this == rhs); }

	idbu16b& operator+=(const idbu16b& rhs);
	idbu16b& operator-=(const idbu16b& rhs);

	idbu16b& operator++() //prefix incr
		{ idbu16b one(0, 1); *this += one; return *this; }
	idbu16b operator++(int) //postfix incr
		{ idbu16b tmp(*this); ++*this; return tmp; }
	idbu16b& operator--() //prefix decr
		{ idbu16b one(0, 1); *this -= one; return *this; }
	idbu16b operator--(int) //postfix decr
		{ idbu16b tmp(*this); --*this; return tmp; }

	// from http://www.boost.org/doc/libs/1_55_0/libs/functional/hash/examples/point.cpp
	friend size_t hash_value(const idbu16b& in)
		{ size_t seed=0; boost::hash_combine(seed, in.lo); boost::hash_combine(seed, in.hi); return seed; }

	std::string toString() const;

	// Requires the underlying 16 bytes to be in host (LE) order
	std::string toUUIDString() const;

	// Requires the underlying 16 bytes to be in network (BE) order
	std::string toIPv6String() const;

	// The resulting 16 bytes are in host (LE) order
	static idbu16b fromUUIDString(const std::string&);

	// The resulting 16 bytes are in network (BE) order
	static idbu16b fromIPv6String(const std::string&);

	bool isNull() const { return ( lo == joblist::BIN16NULLLOW && hi == joblist::BIN16NULLHIGH ); }
	bool isEmpty() const { return ( lo == joblist::BIN16EMPTYROWLOW && hi == joblist::BIN16EMPTYROWHIGH ); }

	void markNull() { lo = joblist::BIN16NULLLOW; hi = joblist::BIN16NULLHIGH; }
	void markEmpty() { lo = joblist::BIN16EMPTYROWLOW; hi = joblist::BIN16EMPTYROWHIGH; }

	//these need to be in LE order s/t returning a casted pointer works
	uint64_t lo;
	uint64_t hi;

protected:

private:
	//defaults okay
	//idbu16b(const idbu16b&);
	//idbu16b& operator=(const idbu16b&);

};

inline idbu16b operator+(const idbu16b& lhs, const idbu16b& rhs) { idbu16b temp(lhs); return temp += rhs; }
inline idbu16b operator-(const idbu16b& lhs, const idbu16b& rhs) { idbu16b temp(lhs); return temp -= rhs; }
inline std::ostream& operator<<(std::ostream& os, const idbu16b& rhs)
	{ os << rhs.toString(); return os; }

}

