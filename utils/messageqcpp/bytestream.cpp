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
*   $Id: bytestream.cpp 2488 2011-04-06 21:23:32Z rdempsey $
*
*
***********************************************************************/
#include <cstring>
#include <stdexcept>
#include <string>
#include <iostream>
#include <fstream>
#include <cassert>
#include <algorithm>
#include <cctype>
using namespace std;

#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/version.hpp>
using namespace boost;

#define BYTESTREAM_DLLEXPORT
#include "bytestream.h"
#undef BYTESTREAM_DLLEXPORT

#define DEBUG_DUMP_STRINGS_LESS_THAN 0

namespace messageqcpp {

/* Copies only the data left to be read */
void ByteStream::doCopy(const ByteStream &rhs)
{
	uint rlen = rhs.length();

	if (fMaxLen < rlen) {
		delete [] fBuf;
		fBuf = new byte[rlen + ISSOverhead];
		fMaxLen = rlen;
	}

	memcpy(fBuf + ISSOverhead, rhs.fCurOutPtr, rlen);
	fCurInPtr = fBuf + ISSOverhead + rlen;
	fCurOutPtr = fBuf + ISSOverhead;
}

ByteStream::ByteStream(const ByteStream& rhs) :
	fBuf(0), fCurInPtr(0), fCurOutPtr(0), fMaxLen(0)
{
	//don't need to copy an empty ByteStream
	if (rhs.fBuf)
		doCopy(rhs);
}

ByteStream::ByteStream(const SBS &rhs) :
	fBuf(0), fCurInPtr(0), fCurOutPtr(0), fMaxLen(0)
{
	if (rhs->fBuf)
		doCopy(*rhs);
}

ByteStream& ByteStream::operator=(const ByteStream& rhs)
{
	if (this != &rhs)
	{
		if (rhs.fBuf)
			doCopy(rhs);
		else
		{
			delete [] fBuf;
			fBuf = fCurInPtr = fCurOutPtr = 0;
			fMaxLen = 0;
		}
	}

	return *this;
}

ByteStream::ByteStream(uint32_t initSize) :
	fBuf(0), fCurInPtr(0), fCurOutPtr(0), fMaxLen(0)
{
	if (initSize > 0) growBuf(initSize);
}

void ByteStream::add(const byte b)
{
 	if (fBuf == 0 || (static_cast<uint32_t>(fCurInPtr - fBuf) == fMaxLen + ISSOverhead))
		growBuf();

	*fCurInPtr++ = b;
}

void ByteStream::growBuf(uint32_t toSize)
{
	if (fBuf == 0)
	{
		if (toSize == 0)
			toSize = BlockSize;
		else
			toSize = ((toSize + BlockSize - 1) / BlockSize) * BlockSize;
		fBuf = new byte[toSize + ISSOverhead];
		fMaxLen = toSize;
		fCurInPtr =
			fCurOutPtr = fBuf + ISSOverhead;
	}
	else
	{
		if (toSize == 0)
			toSize = fMaxLen + BlockSize;
		else
			toSize = ((toSize + BlockSize - 1) / BlockSize) * BlockSize;
		if (toSize <= fMaxLen)
			return;

		// Make sure we at least double the allocation
		toSize = std::max(toSize, fMaxLen * 2);

		byte* t = new byte[toSize + ISSOverhead];
		uint32_t curOutOff = fCurOutPtr - fBuf;
		uint32_t curInOff = fCurInPtr - fBuf;
		memcpy(t, fBuf, fCurInPtr - fBuf);
		delete [] fBuf;
		fBuf = t;
		fMaxLen = toSize;
		fCurInPtr = fBuf + curInOff;
		fCurOutPtr = fBuf + curOutOff;
	}
}

ByteStream& ByteStream::operator<<(const byte b)
{
	add(b);

	return *this;
}

ByteStream& ByteStream::operator<<(const quadbyte q)
{
	if (fBuf == 0 || (fCurInPtr - fBuf + 4U > fMaxLen + ISSOverhead))
		growBuf(fMaxLen + BlockSize);
	*((quadbyte *) fCurInPtr) = q;
 	fCurInPtr += 4;

	return *this;
}

ByteStream& ByteStream::operator<<(const octbyte o)
{

	if (fBuf == 0 || (fCurInPtr - fBuf + 8U > fMaxLen + ISSOverhead))
		growBuf(fMaxLen + BlockSize);
	*((octbyte *) fCurInPtr) = o;
 	fCurInPtr += 8;

	return *this;
}

ByteStream& ByteStream::operator<<(const string& s)
{
	quadbyte len = s.size();

	*this << len;
#if DEBUG_DUMP_STRINGS_LESS_THAN > 0
	if (len < DEBUG_DUMP_STRINGS_LESS_THAN)
	{
		cerr << "bs: appending string len " << len << ": ";
		for (size_t i = 0; i < len; i++)
		{
			char xxx=s.c_str()[i];
			if (isprint(xxx)) cerr << xxx << ' ';
			else cerr << "0x" << hex << ((unsigned)xxx&0xff) << dec << ' ';
		}
		cerr << endl;
	}
#endif
	append(reinterpret_cast<const byte*>(s.c_str()), len);

	return *this;
}

ByteStream& ByteStream::operator<<(const doublebyte d)
{

	if (fBuf == 0 || (fCurInPtr - fBuf + 2U > fMaxLen + ISSOverhead))
		growBuf(fMaxLen + BlockSize);
	*((doublebyte *) fCurInPtr) = d;
 	fCurInPtr += 2;

	return *this;
}

ByteStream& ByteStream::operator>>(byte& b)
{
	peek(b);
	fCurOutPtr++;
	return *this;
}

ByteStream& ByteStream::operator>>(doublebyte& d)
{
	peek(d);
	fCurOutPtr += 2;
	return *this;
}

ByteStream& ByteStream::operator>>(quadbyte& q)
{
	peek(q);
	fCurOutPtr += 4;
	return *this;
}

ByteStream& ByteStream::operator>>(octbyte& o)
{
	peek(o);
	fCurOutPtr += 8;
	return *this;
}

ByteStream& ByteStream::operator>>(string& s)
{
	peek(s);
	fCurOutPtr += 4 + s.length();
	return *this;
}

ByteStream& ByteStream::operator>>(byte*& bpr)
{
	peek(bpr);
	restart();
	return *this;
}

void ByteStream::peek(byte& b) const
{

	if (length() < 1)
		throw underflow_error("ByteStream::peek(byte): not enough data in stream to fill datatype");

	b = *fCurOutPtr;
}

void ByteStream::peek(doublebyte& d) const
{

	if (length() < 2)
		throw underflow_error("ByteStream>doublebyte: not enough data in stream to fill datatype");

	d = *((doublebyte *) fCurOutPtr);
}

void ByteStream::peek(quadbyte& q) const
{

	if (length() < 4)
		throw underflow_error("ByteStream>quadbyte: not enough data in stream to fill datatype");
	
	q = *((quadbyte *) fCurOutPtr);

}

void ByteStream::peek(octbyte& o) const
{

	if (length() < 8)
		throw underflow_error("ByteStream>octbyte: not enough data in stream to fill datatype");

	o = *((octbyte *) fCurOutPtr);
}

void ByteStream::peek(string& s) const
{
	quadbyte len;

	peek(len);
#if DEBUG_DUMP_STRINGS_LESS_THAN > 0
	if (len < DEBUG_DUMP_STRINGS_LESS_THAN)
	{
		cerr << "bs: reading string len " << len << ": ";
		for (size_t i = 0; i < len; i++)
		{
			char xxx = fCurOutPtr[4+i];
			if (isprint(xxx)) cerr << xxx << ' ';
			else cerr << "0x" << hex << ((unsigned)xxx&0xff) << dec << ' ';
		}
		cerr << endl;
	}
#endif
	if (length() < len)
	{
#if DEBUG_DUMP_STRINGS_LESS_THAN > 0
		cerr << "bs: wanted " << len << " bytes, but there are only " << length() << " remaining" << endl;
#endif
		// "put back" the qbyte we just read for strong exception guarantee
		throw underflow_error("ByteStream>string: not enough data in stream to fill datatype");
	}

	scoped_array<char> buf(new char[len + 1]);
	memcpy(buf.get(), &fCurOutPtr[4], len);
	buf[len] = 0;

	s.assign(buf.get(), buf.get() + len);
}

void ByteStream::load(const byte* bp, uint32_t len)
{
	// Do all the stuff that could throw an exception first
	if (bp == 0 && len != 0)
		throw invalid_argument("ByteStream::load: bp cannot equal 0 when len is not equal to 0");

	uint32_t newMaxLen = (len + BlockSize - 1) / BlockSize * BlockSize;

	if (len > fMaxLen) {
		delete [] fBuf;
		fBuf = new byte[newMaxLen + ISSOverhead];
		fMaxLen = newMaxLen;
	}

	memcpy(fBuf + ISSOverhead, bp, len);
	fCurOutPtr = fBuf + ISSOverhead;
	fCurInPtr = fBuf + len + ISSOverhead;
}

void ByteStream::append(const byte* bp, uint32_t len)
{
	if (len == 0)
		return;
	if (bp == 0)
		throw invalid_argument("ByteStream::append: bp cannot equal 0 when len is not equal to 0");

	uint32_t newSize = static_cast<uint32_t>(fCurInPtr - fBuf + len);

	if (fBuf == 0 || (newSize > fMaxLen))
		growBuf(newSize);

	memcpy(fCurInPtr, bp, len);
	fCurInPtr += len;
}

void ByteStream::swap(ByteStream& rhs)
{
	std::swap(fBuf, rhs.fBuf);
	std::swap(fCurInPtr, rhs.fCurInPtr);
	std::swap(fCurOutPtr, rhs.fCurOutPtr);
	std::swap(fMaxLen, rhs.fMaxLen);
}

ifstream& operator>>(ifstream& ifs, ByteStream& bs)
{
	int ifs_len;
	ifs.seekg(0, ios::end);
	ifs_len = ifs.tellg();
	ifs.seekg(0, ios::beg);
	boost::scoped_array<char> buf(new char[ifs_len]);
	ifs.read(buf.get(), ifs_len);
	bs.append(reinterpret_cast<const ByteStream::byte*>(buf.get()), ifs_len);
	return ifs;
}

bool ByteStream::operator==(const ByteStream& b) const 
{
	if (b.length() != length())
		return false;

	return (memcmp(fCurOutPtr, b.fCurOutPtr, length()) == 0);
}

bool ByteStream::operator!=(const ByteStream& b) const 
{
	return !(*this == b);
}

/* Serializeable interface */
void ByteStream::serialize(ByteStream &bs) const
{
	bs << length();
	bs.append(buf(), length());
}

void ByteStream::deserialize(ByteStream &bs)
{
	uint32_t len;

	restart();
	bs >> len;
	load(bs.buf(), len);
	bs.advance(len);
}

void ByteStream::needAtLeast(size_t amount)
{
	size_t currentSpace;

	currentSpace = fMaxLen - (fCurInPtr - (fBuf + ISSOverhead));
	if (currentSpace < amount)
		growBuf(fMaxLen + amount);
}

#ifdef _MSC_VER
#if BOOST_VERSION < 104500
ByteStream& ByteStream::operator<<(const uint ui)
{
	if (fBuf == 0 || (fCurInPtr - fBuf + 4U > fMaxLen + ISSOverhead))
		growBuf(fMaxLen + BlockSize);
	quadbyte q = static_cast<quadbyte>(ui);
	*((quadbyte *) fCurInPtr) = q;
 	fCurInPtr += 4;

	return *this;
}

ByteStream& ByteStream::operator>>(uint& ui)
{
	quadbyte q;
	peek(q);
	fCurOutPtr += 4;
	ui = static_cast<uint>(q);
	return *this;
}
#endif
#endif
}//namespace messageqcpp

