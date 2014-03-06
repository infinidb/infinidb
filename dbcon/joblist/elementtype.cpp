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
 * $Id: elementtype.cpp 9655 2013-06-25 23:08:13Z xlou $
 */

#include <iostream>
#include <utility>
#include <string>
#include <sstream>
using namespace std;

#include "elementtype.h"
using namespace joblist;

namespace joblist
{

StringElementType::StringElementType() { };

StringElementType::StringElementType(uint64_t f, const std::string &s) : first(f), second(s) { };

DoubleElementType::DoubleElementType() { };

DoubleElementType::DoubleElementType(uint64_t f, double s) : first(f), second(s) { };

RIDElementType::RIDElementType() {first = static_cast<uint64_t>(-1); };

RIDElementType::RIDElementType(uint64_t f) : first(f) { };

const string ElementType::toString() const
{
	ostringstream oss;
	oss << first << '/' << second;
	return oss.str();
}

istream& operator>>(istream& in, ElementType& rhs)
{
	in.read((char *) &rhs, sizeof(ElementType));
 	return in;
}

ostream& operator<<(ostream& out, const ElementType& rhs)
{
	out.write((char *) &rhs, sizeof(ElementType));
	return out;
}

ostream& operator<<(std::ostream& out, const StringElementType& rhs)
{
    uint64_t r = rhs.first;
	int16_t dlen = rhs.second.length();

	out.write((char*)&r, sizeof(r));
	out.write( (char*)&dlen, sizeof(dlen));
	out.write( rhs.second.c_str(), dlen);

	return out;
}

istream& operator>>(std::istream& out, StringElementType& rhs)
{
    uint64_t r;
	int16_t dlen;
	char d[32768];   // atm 32k is the largest possible val for the length of strings stored

	out.read((char*)&r, sizeof(r));
	out.read( (char*)&dlen, sizeof(dlen));
	out.read( (char*)&d, dlen);

	rhs.first = r;
	rhs.second = string(d, dlen);

	return out;
}

istream& operator>>(istream& in, DoubleElementType& rhs)
{
	in.read((char *) &rhs, sizeof(DoubleElementType));
	return in;
}

ostream& operator<<(ostream& out, const DoubleElementType& rhs)
{
	out.write((char *) &rhs, sizeof(DoubleElementType));
	return out;
}

istream& operator>>(istream &is, RIDElementType &dl)
{
    is.read((char *)&dl, sizeof(RIDElementType));
    return is;
}

ostream& operator<<(ostream &os, const RIDElementType &dl)
{
    os.write((char *)&dl, sizeof(RIDElementType));
    return os;
}

istream& operator>>(istream &is, TupleType &dl)
{
    throw std::logic_error("TupleType >> not implemented");
}

ostream& operator<<(ostream &os, const TupleType &dl)
{
    throw std::logic_error("TupleType << not implemented");
}

} // namespace joblist

