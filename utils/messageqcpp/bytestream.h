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

/******************************************************************************************
* $Id: bytestream.h 3137 2012-06-07 14:26:39Z rdempsey $
*
*
******************************************************************************************/
/** @file */
#ifndef MESSAGEQCPP_BYTESTREAM_H
#define MESSAGEQCPP_BYTESTREAM_H

#include <string>
#include <iostream>
#include <sys/types.h>
#include <stdexcept>
#include <vector>
#include <set>
#include <boost/shared_ptr.hpp>
#include <boost/version.hpp>
#include <stdint.h>
#include <cstring>

#include "exceptclasses.h"
#include "serializeable.h"

class ByteStreamTestSuite;

#if defined(_MSC_VER) && defined(xxxBYTESTREAM_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace messageqcpp {

typedef boost::shared_ptr<ByteStream> SBS;

/**
 * @brief A class to marshall bytes as a stream
 *
 * The ByteStream class is used to marshall numeric data into and out of a stream of
 * bytes (unsigned chars). It is a FIFO queue that maintains current input and output
 * pointers.
 *
 * @warning Alas, due to recent changes this class no longer implements a strong execption guarantee.
 *
 * @warning the current implementation does not know how to compact memory, so it should
 * destructed or have reset() called to clear out the current byte array. Also, multi-byte
 * numeric values are pushed and dequeued in the native byte order, so they are not portable
 * across machines with different byte orders.
 *
 */
class ByteStream : public Serializeable
{
public:
	/**
	 *	an 8-bit unsigned
	 */
	typedef uint8_t byte;
	/**
	 *	a 16-bit unsigned
	 */
	typedef uint16_t doublebyte;
	/**
	 *	a 32-bit unsigned
	 */
	typedef uint32_t quadbyte;
	/**
	 *	a 64-bit unsigned
	 */
	typedef uint64_t octbyte;

	/**
	 *	default ctor
	 */
	EXPORT explicit ByteStream(uint32_t initSize=8192);   // multiples of pagesize are best
	/**
	 *	ctor with a byte array and len initializer
	 */
	inline ByteStream(const byte* bp, const uint32_t len);
	/**
	 *	copy ctor
	 */
	EXPORT ByteStream(const ByteStream& rhs);
	EXPORT ByteStream(const SBS &rhs);
	/**
	 *	assign op
	 */
	EXPORT ByteStream& operator=(const ByteStream& rhs);
	/**
	 *	assign op
	 */
	inline ByteStream& operator=(const SBS &rhs);
	/**
	 *	dtor
	 */
	inline virtual ~ByteStream();

	/**
	 *	push a byte onto the end of the stream
	 */
	EXPORT ByteStream& operator<<(const byte b);
	/**
	 *	push a doublebyte onto the end of the stream. The byte order is whatever the native byte order is.
	 */
	EXPORT ByteStream& operator<<(const doublebyte d);
	/**
	 *	push a quadbyte onto the end of the stream. The byte order is whatever the native byte order is.
	 */
	EXPORT ByteStream& operator<<(const quadbyte q);
#ifdef _MSC_VER
#if BOOST_VERSION < 104500
	//These 2 are to make MS VC++ w/ boost < 1.45 happy
	EXPORT ByteStream& operator<<(const uint ui);

	EXPORT ByteStream& operator>>(uint& ui);
#endif
#endif
	/**
	 *	push an octbyte onto the end of the stream. The byte order is whatever the native byte order is.
	 */
	EXPORT ByteStream& operator<<(const octbyte o);
	/**
	 * push a std::string onto the end of the stream.
	 */
	EXPORT ByteStream& operator<<(const std::string& s);
	/**
	 * push an arbitrary class onto the end of the stream.
	 */
	inline ByteStream& operator<<(const Serializeable& s);
	/**
	 * push a ByteStream onto the end of the stream.
	 */
	EXPORT ByteStream& operator<<(const ByteStream& bs);

	/**
	 *	extract a byte from the front of the stream.
	 */
	EXPORT ByteStream& operator>>(byte& b);
	/**
	 *	extract a doublebyte from the front of the stream. The byte order is whatever the native byte order is.
	 */
	EXPORT ByteStream& operator>>(doublebyte& d);
	/**
	 *	extract a quadbyte from the front of the stream. The byte order is whatever the native byte order is.
	 */
	EXPORT ByteStream& operator>>(quadbyte& q);

	/**
	 *	extract an octbyte from the front of the stream. The byte order is whatever the native byte order is.
	 */
	EXPORT ByteStream& operator>>(octbyte& o);
	/**
	 * extract a std::string from the front of the stream.
	 */
	EXPORT ByteStream& operator>>(std::string& s);
	/**
	 *	write the current stream into b. The ByteStream will be empty after this operation.
	 * @warning the caller is responsible for making sure b is big enough to hold all the data (perhaps by
	 * calling length()).
	 */
	EXPORT ByteStream& operator>>(byte*& b);
	/**
	 * extract an arbitrary object from the front of the stream.
	 */
	inline ByteStream& operator>>(Serializeable& s);
	/**
	 * extract a ByteStream from the front of the stream.
	 */
	EXPORT ByteStream& operator>>(ByteStream& bs);

	/**
	 *	Peek at a byte from the front of the stream.
	 */
	EXPORT void peek(byte& b) const;
	/**
	 *	Peek at a doublebyte from the front of the stream. The byte order is whatever the native byte order is.
	 */
	EXPORT void peek(doublebyte& d) const;
	/**
	 *	Peek at a quadbyte from the front of the stream. The byte order is whatever the native byte order is.
	 */
	EXPORT void peek(quadbyte& q) const;
	/**
	 *	Peek at an octbyte from the front of the stream. The byte order is whatever the native byte order is.
	 */
	EXPORT void peek(octbyte& o) const;
	/**
	 * Peek at a std::string from the front of the stream.
	 */
	EXPORT void peek(std::string& s) const;
	/**
	 * Peek at the whole ByteStream buffer.
	 * @warning the caller is responsible for making sure b is big enough to hold all the data (perhaps by
	 * calling length()).
	 */
	inline void peek(byte*& b) const;
	/**
	 * Peek at a ByteStream from the front of the stream.
	 */
	EXPORT void peek(ByteStream& bs) const;

	/**
	 *	load the stream from an array. Clears out any previous data.
	 */
	EXPORT void load(const byte* bp, uint32_t len);

	/**
	 *	append bytes to the end of the stream.
	 */
	EXPORT void append(const byte* bp, uint32_t len);

	/**
	 * equality check on buffer contents.
	 */
	EXPORT bool operator==(const ByteStream& b) const;

	/**
	 * inequality check on buffer contents.
	 */
	EXPORT bool operator!=(const ByteStream& b) const;

	/**
	 * these fcns are an alternative to code like {build a msg, bs.load or bs.append}.
	 * This will let us build the msg directly in the BS buffer.
	 */
	EXPORT void needAtLeast(size_t amount);
	inline byte *getInputPtr();
	inline void advanceInputPtr(size_t amount);

	/**
	 *	returns a const pointer to the current head of the queue.  If you use it for
	 *  raw access, you might want to advance the current head.
	 */
	inline const byte* buf() const;

	/**
	 *	returns a pointer to the current head of the queue.  If you use it for
	 *  raw access, you might want to advance the current head.
	 */
	inline byte* buf();

	/**
	 *	advance the output ptr without having to extract bytes
	 * @warning be careful advancing near 4GB!
	 */
	inline void advance(uint32_t amt);

	/**
	 *	returns the length of the queue (in bytes)
	 * @warning do not attempt to make a ByteStream bigger than 4GB!
	 */
	inline uint32_t length() const;

	/**
	 *	returns the length of the queue, including header overhead (in bytes)
	 */
	inline uint32_t lengthWithHdrOverhead() const;

	/**
	 *	clears the stream. Releases any current stream and sets all pointers to 0. The state of the object
	 * is identical to its state immediately after default construction.
	 */
	inline void reset();

	/**
	 * operator+=
	 */
	inline ByteStream& operator+=(const ByteStream& rhs);

	/**
	 * swap this ByteStream with another ByteStream
	 */
	EXPORT void swap(ByteStream& rhs);

	/**
	 * reset the input & output pointers to the beginning
	 */
	inline void restart();

	/**
	 * Move the input pointer back to the beginning so the contents
	 * can be read again.
	 */
	inline void rewind();

	/**
	 * Serializeable interface
	 */
	EXPORT void serialize(ByteStream &bs) const;
	
	/**
	 * Serializeable interface
	 */
	EXPORT void deserialize(ByteStream &bs);

	/**
	 *	memory allocation chunk size
	 */
	EXPORT static const uint32_t BlockSize = 4096;

	/** size of the space we want in front of the data */
	EXPORT static const uint ISSOverhead = 2*sizeof(uint32_t);  //space for the BS magic & length

	friend class ::ByteStreamTestSuite;

protected:
	/**
	 *	pushes one byte onto the end of the stream
	 */
	void add(const byte b);
	/**
	 *	adds another BlockSize bytes to the internal buffer
	 */
	void growBuf(uint32_t toSize=0);
	/**
	 *	handles member copying from one ByteStream to another
	 */
	void doCopy(const ByteStream& rhs);

private:

	byte* fBuf; ///the start of the allocated buffer
	byte* fCurInPtr; //the point in fBuf where data is inserted next
	byte* fCurOutPtr; //the point in fBuf where data is extracted from next
	uint32_t fMaxLen; //how big fBuf is currently
};

inline ByteStream::ByteStream(const byte* bp, const uint32_t len) : fBuf(0), fMaxLen(0) { load(bp, len); }
inline ByteStream::~ByteStream() { delete [] fBuf; }

inline const ByteStream::byte* ByteStream::buf() const { return fCurOutPtr; }
inline ByteStream::byte* ByteStream::buf() { return fCurOutPtr; }
inline uint32_t ByteStream::length() const { return (uint32_t)(fCurInPtr - fCurOutPtr); }
inline uint32_t ByteStream::lengthWithHdrOverhead() const
	{return (length() + ISSOverhead);}
inline void ByteStream::reset() { delete [] fBuf; fMaxLen = 0; 
	fCurInPtr = fCurOutPtr = fBuf = 0; }
inline void ByteStream::restart() { fCurInPtr = fCurOutPtr = fBuf + ISSOverhead; }
inline void ByteStream::rewind() { fCurOutPtr = fBuf + ISSOverhead; }
inline void ByteStream::advance(uint32_t adv) 
{
	//fCurOutPtr is always >= fBuf, so fCurOutPtr - fBuf is >= 0, and this difference is always <= 32 bits
	//there is an edge condition not detected here: if fCurOutPtr - fBuf is nearly 4GB and you try to
	//advance by a lot, you will wrap over, so be warned!
	if (adv > length())
		throw std::length_error("ByteStream: advanced beyond the end of the buffer");
	fCurOutPtr += adv; 
}
inline ByteStream::byte* ByteStream::getInputPtr() { return fCurInPtr; }
inline void ByteStream::advanceInputPtr(size_t amount) { fCurInPtr += amount; }
inline void ByteStream::peek(byte*& bpr) const { memcpy(bpr, fCurOutPtr, length()); }

inline ByteStream& ByteStream::operator+=(const ByteStream& rhs) { append(rhs.buf(), rhs.length()); return *this; }
inline ByteStream operator+(const ByteStream& lhs, const ByteStream& rhs) { ByteStream temp(lhs); return temp += rhs; }
inline ByteStream& ByteStream::operator>>(Serializeable& s) { s.deserialize(*this); return *this; }
inline ByteStream& ByteStream::operator<<(const Serializeable& s) { s.serialize(*this); return *this; }
inline ByteStream& ByteStream::operator=(const SBS& rhs) { *this = *rhs; return *this; }

/**
 * stream a ByteStream out to any ostream
 */
inline std::ostream& operator<<(std::ostream& os, const ByteStream& bs)
{
	return os.write(reinterpret_cast<const char*>(bs.buf()), bs.length());
}

/**
 * stream a ByteStream in from a file
 */
EXPORT std::ifstream& operator>>(std::ifstream& os, ByteStream& bs);

/// Generic method to export a vector of T's that implement Serializeable
template<typename T>
void serializeVector(messageqcpp::ByteStream& bs, const std::vector<T>& v)
{
	typename std::vector<T>::const_iterator it;
	uint64_t size;
	
	size = v.size();
	bs << size;
	for (it = v.begin(); it != v.end(); it++)
		bs << *it;
}

/// Generic method to deserialize a vector of T's that implement Serializeable
template<typename T>
void deserializeVector(messageqcpp::ByteStream& bs, std::vector<T>& v)
{
	uint i;
	T tmp;
	uint64_t size;
	
	v.clear();
	bs >> size;
	for (i = 0; i < size; i++) {
		bs >> tmp;
		v.push_back(tmp);
	}
}

#ifdef _MSC_VER
//Until the API is fixed to be 64-bit clean...
#pragma warning (push)
#pragma warning (disable : 4267)
#endif

template<typename T>
void serializeInlineVector(messageqcpp::ByteStream &bs, const std::vector<T> &v)
{
	uint64_t size = v.size();
	bs << size;
	if (size > 0)
		bs.append((const uint8_t *) &(v[0]), sizeof(T) * size);
}

inline void serializeVector(ByteStream& bs, const std::vector<int64_t>& v) { serializeInlineVector<int64_t>(bs, v); }

template<typename T>
void deserializeInlineVector(messageqcpp::ByteStream &bs, std::vector<T> &v)
{
	uint64_t size;
	const uint8_t *buf;

	v.clear();
	bs >> size;
	if (size > 0)
	{
		v.resize(size);
		buf = bs.buf();
		memcpy(&(v[0]), buf, sizeof(T) * size);
		bs.advance(sizeof(T) * size);
	}
}

#ifdef _MSC_VER
#pragma warning (pop)
#endif

inline void deserializeVector(ByteStream& bs, std::vector<int64_t>& v) { deserializeInlineVector<int64_t>(bs, v); }

/// Generic method to serialize a set of T's that implement Serializeable
template<typename T>
void serializeSet(messageqcpp::ByteStream &bs, const std::set<T> &s)
{
	uint64_t size = s.size();
	bs << size;
	typename std::set<T>::const_iterator it;
	for (it = s.begin(); it != s.end(); ++it)
		bs << *it;
}

/// Generic method to deserialize a set of T's that implement Serializeable
template<typename T>
void deserializeSet(messageqcpp::ByteStream& bs, std::set<T>& s)
{
	uint i;
	T tmp;
	uint64_t size;
	
	s.clear();
	bs >> size;
	for (i = 0; i < size; i++) {
		bs >> tmp;
		s.insert(tmp);
	}
}

}//namespace messageqcpp

namespace std
{
	/** total specialization of std::swap
	 *
	 */
	template<> inline void swap<messageqcpp::ByteStream>(messageqcpp::ByteStream& lhs, messageqcpp::ByteStream&rhs)
	{
		lhs.swap(rhs);
	}
}//namespace std

#undef EXPORT

#endif //MESSAGEQCPP_BYTESTREAM_H
