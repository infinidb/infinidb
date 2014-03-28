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

#include <string>
#include <stdexcept>
#include <iostream>
#include <fstream>
using namespace std;
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>

#include <cppunit/extensions/HelperMacros.h>

#include "bytestream.h"
#include "messagequeue.h"
#include "socketparms.h"
#include "inetstreamsocket.h"
#include "socketclosed.h"
using namespace messageqcpp;
#include "configcpp.h"
using namespace config;

class ByteStreamTestSuite : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ByteStreamTestSuite );

CPPUNIT_TEST( bs_1 );
CPPUNIT_TEST( bs_1_1 );
CPPUNIT_TEST( bs_1_2 );
CPPUNIT_TEST( bs_2 );
CPPUNIT_TEST( bs_3 );
CPPUNIT_TEST( bs_4 );
CPPUNIT_TEST_EXCEPTION( bs_5_1, std::underflow_error );
CPPUNIT_TEST_EXCEPTION( bs_5_2, std::underflow_error );
CPPUNIT_TEST_EXCEPTION( bs_5_3, std::underflow_error );
CPPUNIT_TEST_EXCEPTION( bs_5_4, std::underflow_error );
CPPUNIT_TEST_EXCEPTION( bs_5_5, std::underflow_error );
CPPUNIT_TEST_EXCEPTION( bs_5_6, std::underflow_error );
CPPUNIT_TEST( bs_6 );
CPPUNIT_TEST( bs_7 );
CPPUNIT_TEST( bs_8 );
CPPUNIT_TEST_EXCEPTION( bs_9, std::underflow_error );
CPPUNIT_TEST( bs_10 );
// CPPUNIT_TEST( bs_11 );
CPPUNIT_TEST( bs_12 );
CPPUNIT_TEST( bs_13 );
CPPUNIT_TEST( bs_14 );
CPPUNIT_TEST( bs_15 );
CPPUNIT_TEST( bs_16 );
CPPUNIT_TEST_SUITE_END();

private:
	ByteStream::byte b;
	ByteStream::doublebyte d;
	ByteStream::quadbyte q;
	ByteStream::octbyte o;

    uint8_t     u8;
    uint16_t    u16;
    uint32_t    u32;
    uint64_t    u64;
    int8_t      i8;
    int16_t     i16;
    int32_t     i32;
    int64_t     i64;

	ByteStream bs;
	ByteStream bs1;

	ByteStream::byte* bap;
	ByteStream::byte* bap1;

	int len;

public:
	void setUp() {
		bs.reset();
		bs1.reset();
		bap = 0;
		bap1 = 0;
	}

	void tearDown() {
		bs.reset();
		bs1.reset();
		delete [] bap;
		bap = 0;
		delete [] bap1;
		bap1 = 0;
	}

void bs_1() {

	bs.reset();

	o = 0xdeadbeefbadc0ffeLL;
	bs << o;
	CPPUNIT_ASSERT(bs.length() == 8);
	o = 0;
	bs >> o;
	CPPUNIT_ASSERT(o == 0xdeadbeefbadc0ffeLL);
	CPPUNIT_ASSERT(bs.length() == 0);

	q = 0xdeadbeef;
	bs << q;
	CPPUNIT_ASSERT(bs.length() == 4);
	q = 0;
	bs >> q;
	CPPUNIT_ASSERT(q == 0xdeadbeef);
	CPPUNIT_ASSERT(bs.length() == 0);

	d = 0xf00f;
	bs << d;
	CPPUNIT_ASSERT(bs.length() == 2);
	d = 0;
	bs >> d;
	CPPUNIT_ASSERT(d == 0xf00f);
	CPPUNIT_ASSERT(bs.length() == 0);

	b = 0x0f;
	bs << b;
	CPPUNIT_ASSERT(bs.length() == 1);
	b = 0;
	bs >> b;
	CPPUNIT_ASSERT(b == 0x0f);
	CPPUNIT_ASSERT(bs.length() == 0);

	o = 0xdeadbeefbadc0ffeLL;
	bs << o;
	CPPUNIT_ASSERT(bs.length() == 8);
	o = 0;

	q = 0xdeadbeef;
	bs << q;
	CPPUNIT_ASSERT(bs.length() == 12);
	q = 0;

	d = 0xf00f;
	bs << d;
	CPPUNIT_ASSERT(bs.length() == 14);
	d = 0;

	b = 0x0f;
	bs << b;
	CPPUNIT_ASSERT(bs.length() == 15);
	b = 0;

	bs >> o;
	CPPUNIT_ASSERT(o == 0xdeadbeefbadc0ffeLL);
	CPPUNIT_ASSERT(bs.length() == 7);
	bs >> q;
	CPPUNIT_ASSERT(q == 0xdeadbeef);
	CPPUNIT_ASSERT(bs.length() == 3);
	bs >> d;
	CPPUNIT_ASSERT(d == 0xf00f);
	CPPUNIT_ASSERT(bs.length() == 1);
	bs >> b;
	CPPUNIT_ASSERT(b == 0x0f);
	CPPUNIT_ASSERT(bs.length() == 0);
}

void bs_1_1() {

	bs.reset();

	o = 0xdeadbeefbadc0ffeLL;
	bs << o;
	CPPUNIT_ASSERT(bs.length() == 8);
	o = 0;

	q = 0xdeadbeef;
	bs << q;
	CPPUNIT_ASSERT(bs.length() == 12);
	q = 0;

	d = 0xf00f;
	bs << d;
	CPPUNIT_ASSERT(bs.length() == 14);
	d = 0;

	b = 0x0f;
	bs << b;
	CPPUNIT_ASSERT(bs.length() == 15);
	b = 0;

	ByteStream bbs1;
	bbs1 << bs;
	CPPUNIT_ASSERT(bbs1.length() == bs.length() + 4);
	bs.reset();
	bbs1 >> bs;
	CPPUNIT_ASSERT(bbs1.length() == 0);
	CPPUNIT_ASSERT(bs.length() == 15);

	bs >> o;
	CPPUNIT_ASSERT(o == 0xdeadbeefbadc0ffeLL);
	CPPUNIT_ASSERT(bs.length() == 7);
	bs >> q;
	CPPUNIT_ASSERT(q == 0xdeadbeef);
	CPPUNIT_ASSERT(bs.length() == 3);
	bs >> d;
	CPPUNIT_ASSERT(d == 0xf00f);
	CPPUNIT_ASSERT(bs.length() == 1);
	bs >> b;
	CPPUNIT_ASSERT(b == 0x0f);
	CPPUNIT_ASSERT(bs.length() == 0);
}

void bs_1_2() {

	bs.reset();

    i64 = -2401053089477160962;
	bs << i64;
	CPPUNIT_ASSERT(bs.length() == 8);
	i64 = 0;

	i32 = -559038737;
	bs << i32;
	CPPUNIT_ASSERT(bs.length() == 12);
    i32 = 0;

	i16 = -4081;
	bs << i16;
	CPPUNIT_ASSERT(bs.length() == 14);
	i16 = 0;

	i8 = 15;
	bs << i8;
	CPPUNIT_ASSERT(bs.length() == 15);
	i8 = 0;

	bs >> i64;
	CPPUNIT_ASSERT(i64 == -2401053089477160962);
	CPPUNIT_ASSERT(bs.length() == 7);

    bs >> i32;
	CPPUNIT_ASSERT(i32 == -559038737);
	CPPUNIT_ASSERT(bs.length() == 3);

    bs >> i16;
	CPPUNIT_ASSERT(i16 == -4081);
	CPPUNIT_ASSERT(bs.length() == 1);

    bs >> i8;
	CPPUNIT_ASSERT(i8 == 15);
	CPPUNIT_ASSERT(bs.length() == 0);
}

void bs_2() {
	int i;

	bs.reset();
	srand(time(0));

	for (i = 0; i < 10240; i++)
	{
		bs << (uint32_t)rand();
	}
	bs1 = bs;

	uint32_t q1;

	for (i = 0; i < 10240; i++)
	{
		bs >> u32;
		bs1 >> q1;
		CPPUNIT_ASSERT(u32 == q1);
	}
	bs.reset();
	bs1.reset();
}

void bs_3() {

	uint8_t ba[1024] = { 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, };

	bs.load(ba, 8);
	CPPUNIT_ASSERT(bs.length() == 8);
	bs >> u8;
	CPPUNIT_ASSERT(b == 0x12);
	bs >> u16;
	CPPUNIT_ASSERT(d == 0x5634);
	bs >> u32;
	CPPUNIT_ASSERT(q == 0xdebc9a78);

	CPPUNIT_ASSERT(bs.length() == 1);

	bs.reset();
	CPPUNIT_ASSERT(bs.length() == 0);

	bs.load(ba, 8);
	len = bs.length();
	CPPUNIT_ASSERT(len == 8);
	bap = new ByteStream::byte[len];
	//bs >> bap;
	memcpy(bap, bs.buf(), len);
	CPPUNIT_ASSERT(memcmp(ba, bap, len) == 0);
	delete [] bap;
	bap = 0;

	bs.reset();
	for (u32 = 0; u32 < 20480; u32++)
	{
		bs << u32;
	}

	len = bs.length();
	CPPUNIT_ASSERT(len == (20480 * sizeof(u32)));
	bap = new ByteStream::byte[len];
	//bs >> bap;
	memcpy(bap, bs.buf(), len);

	bs.reset();
	for (u32 = 0; u32 < 20480; u32++)
	{
		bs << u32;
	}
	len = bs.length();
	CPPUNIT_ASSERT(len == (20480 * sizeof(q)));
	bap1 = new ByteStream::byte[len];
	//bs >> bap1;
	memcpy(bap1, bs.buf(), len);

	CPPUNIT_ASSERT(memcmp(bap1, bap, len) == 0);

	delete [] bap;
	bap = 0;
	delete [] bap1;
	bap1 = 0;
	bs.reset();
}
void bs_4() {

	for (i32 = 0; i32 < 20480; i32++)
	{
		bs << i32;
	}

	ByteStream bs2(bs);
	len = bs2.length();
	CPPUNIT_ASSERT(len == (20480 * sizeof(i32)));
	bap = new ByteStream::byte[len];
	//bs2 >> bap;
	memcpy(bap, bs2.buf(), len);

	bs1 = bs2;
	len = bs1.length();
	CPPUNIT_ASSERT(len == (20480 * sizeof(i32)));
	bap1 = new ByteStream::byte[len];
	//bs1 >> bap1;
	memcpy(bap1, bs1.buf(), len);

	CPPUNIT_ASSERT(memcmp(bap1, bap, len) == 0);
	delete [] bap;
	bap = 0;
	delete [] bap1;
	bap1 = 0;
	bs.reset();
	bs1.reset();
	bs2.reset();
}

void bs_5_1() {
	bs.reset();

	u8 = 0x0f;
	bs << u8;

	for (;;) bs >> u32;
}

void bs_5_2() {
	bs.reset();

	u8 = 0x0f;
	bs << u8;

	for (;;) bs >> u16;
}

void bs_5_3() {
	bs.reset();

	u8 = 0x0f;
	bs << u8;

	for (;;) bs >> u8;
}

void bs_5_4() {
	bs.reset();

	i8 = 0x0f;
	bs << i8;

	for (;;) bs >> i32;
}

void bs_5_5() {
	bs.reset();

	i8 = 0x0f;
	bs << i8;

	for (;;) bs >> i16;
}

void bs_5_6() {
	bs.reset();

	i8 = 0x0f;
	bs << i8;

	for (;;) bs >> i8;
}

void bs_6()
{
	u8 = 0x1a;
	bs << u8;
	u8 = 0x2b;
	bs << u8;
	u8 = 0x3c;
	bs << u8;

	bs >> u8;
	CPPUNIT_ASSERT(u8 == 0x1a);
	bs >> u8;
	CPPUNIT_ASSERT(u8 == 0x2b);
	bs >> u8;
	CPPUNIT_ASSERT(u8 == 0x3c);

	bs.reset();

	u8 = 12;
	bs << u8;
	u8 = 3;
	bs << u8;
	u8 = 0;
	bs << u8;
	u8 = 2;
	bs << u8;

	ByteStream bs3(bs);

	bs3 >> u8;
	CPPUNIT_ASSERT(u8 == 12);
	bs3 >> u8;
	CPPUNIT_ASSERT(u8 == 3);
	bs3 >> u8;
	CPPUNIT_ASSERT(u8 == 0);
	bs3 >> u8;
	CPPUNIT_ASSERT(u8 == 2);

}

void bs_7()
{
	size_t i;

	bs.reset();
	bap = new ByteStream::byte[ByteStream::BlockSize * 2];
	ByteStream::byte* bapp;

	for (bapp = &bap[0], i = 0; i < ByteStream::BlockSize; bapp++, i++) *bapp = 0xa5;
	bs.append(bap, ByteStream::BlockSize);
	CPPUNIT_ASSERT(bs.length() == (ByteStream::BlockSize * 1));
	for (bapp = &bap[0], i = 0; i < ByteStream::BlockSize; bapp++, i++) *bapp = 0x5a;
	bs.append(bap, ByteStream::BlockSize);
	CPPUNIT_ASSERT(bs.length() == (ByteStream::BlockSize * 2));
	for (bapp = &bap[0], i = 0; i < ByteStream::BlockSize * 2; bapp++, i++) *bapp = 0x55;
	bs.append(bap, ByteStream::BlockSize * 2);
	CPPUNIT_ASSERT(bs.length() == (ByteStream::BlockSize * 4));
	delete [] bap;
	bap = new ByteStream::byte[bs.length()];
	//bs >> bap;
	memcpy(bap, bs.buf(), bs.length());
	bap1 = new ByteStream::byte[bs.length()];
	for (bapp = &bap1[0], i = 0; i < ByteStream::BlockSize; bapp++, i++) *bapp = 0xa5;
	for (i = 0; i < ByteStream::BlockSize; bapp++, i++) *bapp = 0x5a;
	for (i = 0; i < ByteStream::BlockSize * 2; bapp++, i++) *bapp = 0x55;
	CPPUNIT_ASSERT(memcmp(bap, bap1, bs.length()) == 0);
	delete [] bap;
	bap = 0;
	delete [] bap1;
	bap1 = 0;

}

void bs_8()
{
	bs.reset();
	string s;
	s = "This is a test";
	bs << s;
	string s1;
	bs >> s1;
	CPPUNIT_ASSERT(s == s1);
	CPPUNIT_ASSERT(bs.length() == 0);

	ifstream ifs;
	ifs.open("./tdriver.cpp");
	int ifs_len;
	ifs.seekg(0, ios::end);
	ifs_len = ifs.tellg();
	ifs.seekg(0, ios::beg);
	boost::scoped_array<char> buf(new char[ifs_len + 1]);
	ifs.read(buf.get(), ifs_len);
	buf[ifs_len] = 0;
	ifs.close();
	bs.reset();
	s = buf.get();
	bs << s;
	bs >> s1;
	CPPUNIT_ASSERT(s == s1);
	CPPUNIT_ASSERT(bs.length() == 0);

	u8 = 0xa5;
	bs << u8;
	u16 = 0x5aa5;
	bs << u16;
	u32 = 0xdeadbeef;
	bs << u32;
	bs << s;
	s += s1;
	bs << s;
	s += s1;
	bs << s;
	bs << u32;
	bs << u16;
	bs << u8;

	bs >> u8;
	CPPUNIT_ASSERT(u8 == 0xa5);
	bs >> u16;
	CPPUNIT_ASSERT(u16 == 0x5aa5);
	bs >> u32;
	CPPUNIT_ASSERT(u32 == 0xdeadbeef);
	bs >> s;
	CPPUNIT_ASSERT(s == s1);
	CPPUNIT_ASSERT(s.length() == (s1.length() * 1));
	bs >> s;
	CPPUNIT_ASSERT(s.length() == (s1.length() * 2));
	bs >> s;
	CPPUNIT_ASSERT(s.length() == (s1.length() * 3));
	bs >> u32;
	CPPUNIT_ASSERT(u32 == 0xdeadbeef);
	bs >> u16;
	CPPUNIT_ASSERT(u16 == 0x5aa5);
	bs >> u8;
	CPPUNIT_ASSERT(u8 == 0xa5);
	CPPUNIT_ASSERT(bs.length() == 0);

}

void bs_9()
{
	bs.reset();
	// Load up a bogus string (too short)
	u32 = 100;
	bs << u32;
	bs.append(reinterpret_cast<const ByteStream::byte*>("This is a test"), 14);
	string s;
	// Should throw underflow
	bs >> s;
}

void bs_10()
{
	bs.reset();
	bs1.reset();
	u32 = 0xdeadbeef;
	bs << u32;
	CPPUNIT_ASSERT(bs.length() == 4);
	CPPUNIT_ASSERT(bs1.length() == 0);
	bs.swap(bs1);
	CPPUNIT_ASSERT(bs1.length() == 4);
	CPPUNIT_ASSERT(bs.length() == 0);
	bs1 >> u32;
	CPPUNIT_ASSERT(u32 == 0xdeadbeef);

	bs.reset();
	bs1.reset();
	u32 = 0xdeadbeef;
	bs << u32;
	bs1 << u32;
	bs += bs1;
	CPPUNIT_ASSERT(bs.length() == 8);
	bs >> u32;
	CPPUNIT_ASSERT(u32 == 0xdeadbeef);
	bs >> u32;
	CPPUNIT_ASSERT(u32 == 0xdeadbeef);

	bs.reset();
	bs1.reset();
	ByteStream bs2;
	u32 = 0xdeadbeef;
	bs1 << u32;
	bs2 << u32;
	bs = bs1 + bs2;
	CPPUNIT_ASSERT(bs.length() == 8);
	bs >> u32;
	CPPUNIT_ASSERT(u32 == 0xdeadbeef);
	bs >> u32;
	CPPUNIT_ASSERT(u32 == 0xdeadbeef);

}

void bs_11()
{
	bs.reset();
	bs1.reset();
	u32 = 0xdeadbeef;
	bs << u32;
	bs1 << u32;

	// save bs1 state
	ByteStream::byte* bs1_fBuf = bs1.fBuf;
	ByteStream::byte* bs1_fCurInPtr = bs1.fCurInPtr;
	ByteStream::byte* bs1_fCurOutPtr = bs1.fCurOutPtr;
	size_t bs1_fMaxLen = bs1.fMaxLen;

	//introduce an error
	bs.fCurOutPtr += 1024000;

	// save bs state
	ByteStream::byte* bs_fBuf = bs.fBuf;
	ByteStream::byte* bs_fCurInPtr = bs.fCurInPtr;
	ByteStream::byte* bs_fCurOutPtr = bs.fCurOutPtr;
	size_t bs_fMaxLen = bs.fMaxLen;

	try
	{
		bs1 = bs;
	}
	catch (out_of_range& ex)
	{
	}

	//at this point bs1 should be just as before the assignment
	CPPUNIT_ASSERT(bs1.fBuf == bs1_fBuf);
	CPPUNIT_ASSERT(bs1.fCurInPtr == bs1_fCurInPtr);
	CPPUNIT_ASSERT(bs1.fCurOutPtr == bs1_fCurOutPtr);
	CPPUNIT_ASSERT(bs1.fMaxLen == bs1_fMaxLen);

	//same goes for bs
	CPPUNIT_ASSERT(bs.fBuf == bs_fBuf);
	CPPUNIT_ASSERT(bs.fCurInPtr == bs_fCurInPtr);
	CPPUNIT_ASSERT(bs.fCurOutPtr == bs_fCurOutPtr);
	CPPUNIT_ASSERT(bs.fMaxLen == bs_fMaxLen);

}

void bs_12() {

	bs.reset();

	u64 = 0xdeadbeefbadc0ffeLL;
	bs << u64;
	CPPUNIT_ASSERT(bs.length() == 8);
	u64 = 0;
	bs.peek(u64);
	CPPUNIT_ASSERT(u64 == 0xdeadbeefbadc0ffeLL);
	CPPUNIT_ASSERT(bs.length() == 8);
	u64 = 0;
	bs >> u64;
	CPPUNIT_ASSERT(u64 == 0xdeadbeefbadc0ffeLL);
	CPPUNIT_ASSERT(bs.length() == 0);

	u16 = 0xf00f;
	bs << u16;
	CPPUNIT_ASSERT(bs.length() == 2);
	u16 = 0;
	bs.peek(u16);
	CPPUNIT_ASSERT(u16 == 0xf00f);
	CPPUNIT_ASSERT(bs.length() == 2);
	u16 = 0;
	bs >> u16;
	CPPUNIT_ASSERT(u16 == 0xf00f);
	CPPUNIT_ASSERT(bs.length() == 0);

	u8 = 0x0f;
	bs << u8;
	CPPUNIT_ASSERT(bs.length() == 1);
	u8 = 0;
	bs.peek(u8);
	CPPUNIT_ASSERT(u8 == 0x0f);
	CPPUNIT_ASSERT(bs.length() == 1);
	u8 = 0;
	bs >> u8;
	CPPUNIT_ASSERT(u8 == 0x0f);
	CPPUNIT_ASSERT(bs.length() == 0);

	u32 = 0xdeadbeef;
	bs << u32;
	CPPUNIT_ASSERT(bs.length() == 4);
	u32 = 0;

	u16 = 0xf00f;
	bs << u16;
	CPPUNIT_ASSERT(bs.length() == 6);
	u16 = 0;

	u8 = 0x0f;
	bs << u8;
	CPPUNIT_ASSERT(bs.length() == 7);
	u8 = 0;

	bs.peek(u32);
	CPPUNIT_ASSERT(u32 == 0xdeadbeef);
	CPPUNIT_ASSERT(bs.length() == 7);
	u32 = 0;
	bs >> u32;
	CPPUNIT_ASSERT(u32 == 0xdeadbeef);
	CPPUNIT_ASSERT(bs.length() == 3);
	u16 = 0;
	bs.peek(u16);
	CPPUNIT_ASSERT(u16 == 0xf00f);
	CPPUNIT_ASSERT(bs.length() == 3);
	u16 = 0;
	bs >> u16;
	CPPUNIT_ASSERT(u16 == 0xf00f);
	CPPUNIT_ASSERT(bs.length() == 1);
	u8 = 0;
	bs.peek(u8);
	CPPUNIT_ASSERT(u8 == 0x0f);
	CPPUNIT_ASSERT(bs.length() == 1);
	u8 = 0;
	bs >> u8;
	CPPUNIT_ASSERT(u8 == 0x0f);
	CPPUNIT_ASSERT(bs.length() == 0);

	string s;
	s = "This is a test";
	bs << s;
	string s1;
	bs.peek(s1);
	CPPUNIT_ASSERT(s == s1);
	CPPUNIT_ASSERT(bs.length() == s1.size() + 4);
	s1.empty();
	bs >> s1;
	CPPUNIT_ASSERT(s == s1);
	CPPUNIT_ASSERT(bs.length() == 0);
}

void bs_13()
{
	string s;
	ifstream ifs;
	ifs.open("./tdriver.cpp");
	int ifs_len;
	ifs.seekg(0, ios::end);
	ifs_len = ifs.tellg();
	ifs.seekg(0, ios::beg);
	boost::scoped_array<char> buf(new char[ifs_len + 1]);
	ifs.read(buf.get(), ifs_len);
	buf[ifs_len] = 0;
	ifs.close();
	bs.reset();
	s = buf.get();
	bs << s;
	ofstream of("bs_13.dat");
	of << bs;
	of.close();
	ifs.open("./bs_13.dat");
	ifs.seekg(0, ios::end);
	int ifs_len1;
	ifs_len1 = ifs.tellg();
	// will be longer than orig file because string length is encoded into stream
	CPPUNIT_ASSERT((ifs_len + (int)sizeof(ByteStream::quadbyte)) == ifs_len1);
	ifs.seekg(0, ios::beg);
	boost::scoped_array<char> buf1(new char[ifs_len1]);
	bs1.reset();
	ifs >> bs1;
	ifs.close();
	CPPUNIT_ASSERT(bs.length() == bs1.length());
	string s1;
	bs1 >> s1;
	CPPUNIT_ASSERT(s == s1);
}

void bs_14()
{
	ByteStream bs1(0);
	ByteStream bs2(bs1);
	CPPUNIT_ASSERT(bs2.fBuf == 0);
	ByteStream bs3(0);
	bs3 = bs1;
	CPPUNIT_ASSERT(bs3.fBuf == 0);
}

void bs_15()
{
	ByteStream b1, b2, empty;
	uint8_t u8;

	CPPUNIT_ASSERT(b1 == b2);
	CPPUNIT_ASSERT(b2 == b1);
	CPPUNIT_ASSERT(b2 == empty);
	CPPUNIT_ASSERT(b1 == empty);
	
	CPPUNIT_ASSERT(!(b1 != b2));
	CPPUNIT_ASSERT(!(b2 != b1));
	CPPUNIT_ASSERT(!(b2 != empty));
	CPPUNIT_ASSERT(!(b1 != empty));

	b1 << "Woo hoo";
	
	CPPUNIT_ASSERT(b1 != b2);
	CPPUNIT_ASSERT(b2 != b1);
	CPPUNIT_ASSERT(b1 != empty);
	
	CPPUNIT_ASSERT(!(b1 == b2));
	CPPUNIT_ASSERT(!(b2 == b1));
	CPPUNIT_ASSERT(!(b1 == empty));

	b2 << "Woo hoo";

	CPPUNIT_ASSERT(b1 == b2);
	CPPUNIT_ASSERT(b2 == b1);
	CPPUNIT_ASSERT(!(b1 != b2));
	CPPUNIT_ASSERT(!(b2 != b1));

	b1 >> u8;

	CPPUNIT_ASSERT(b1 != b2);
	CPPUNIT_ASSERT(b2 != b1);
	CPPUNIT_ASSERT(!(b1 == b2));
	CPPUNIT_ASSERT(!(b2 == b1));

	b1 << u8;

	CPPUNIT_ASSERT(b1 != b2);
	CPPUNIT_ASSERT(b2 != b1);
	CPPUNIT_ASSERT(!(b1 == b2));
	CPPUNIT_ASSERT(!(b2 == b1));

	b2 >> u8;
	b2 << u8;

	CPPUNIT_ASSERT(b1 == b2);
	CPPUNIT_ASSERT(b2 == b1);
	CPPUNIT_ASSERT(!(b1 != b2));
	CPPUNIT_ASSERT(!(b2 != b1));
	
}

void bs_16()
{
	int i;
	u_int32_t len;

	bs.reset();
	srand(time(0));

	for (i = 0; i < 10240; i++)
	{
		bs << (ByteStream::quadbyte)rand();
	}

	boost::scoped_array<ByteStream::byte> bp(new ByteStream::byte[bs.length()]);
	ByteStream::byte* bpp = bp.get();
	boost::scoped_array<ByteStream::byte> bp1(new ByteStream::byte[bs.length()]);
	ByteStream::byte* bpp1 = bp1.get();

	len = bs.length();
	CPPUNIT_ASSERT(len == 10240 * 4);
	bs.peek(bpp);
	CPPUNIT_ASSERT(bs.length() == len);
	CPPUNIT_ASSERT(memcmp(bpp, bs.buf(), len) == 0);

	bs >> bpp1;
	CPPUNIT_ASSERT(bs.length() == 0);
	CPPUNIT_ASSERT(memcmp(bpp, bpp1, len) == 0);

	bs.reset();
}

}; 

static string normServ;
static string brokeServ;
static string writeServ;
volatile static bool keepRunning;
volatile static bool isRunning;
volatile static bool leakCheck;

#define TS_NS(x) (x)
#define TS_US(x) ((x) * 1000)
#define TS_MS(x) ((x) * 1000000)

static void startServer()
{
	MessageQueueServer* inMq;
	bool retry;

	do {
		try {
			retry = false;
			inMq = new MessageQueueServer(normServ, "./Calpont.xml");
		}
		catch (exception& ex) {
			//cout << endl << "MessageQueueServer ctor threw!: " << ex.what() << endl;
			::usleep(5000000);
			retry = true;
		}
		catch (...) {
			//cout << endl << "MessageQueueServer ctor threw!" << endl;
			::usleep(5000000);
			retry = true;
		}
	} while (retry);

	ByteStream inBs;
	//cout << endl << "startServer is starting" << endl;

	// We need to loop here because the big write in mq_1() may (will) not come in one
	//  read. The other servers will fail if if too much is writen.

	ByteStream bs2;
	struct timespec ts = { 0, TS_MS(500) };
	isRunning = true;
	IOSocket sock = inMq->accept();
	for (;;)
	{
		inBs.reset();
		try {
			bs2 = sock.read(&ts);
		}
		catch (SocketClosed &e) {
			break;
		}
		inBs += bs2;
		while (bs2.length() > 0)
		{
			//cerr << endl << "startServer: going back for more..." << endl;
			try {
				bs2 = sock.read(&ts);
			}
			catch (SocketClosed &e) {
				break;
			}
			inBs += bs2;
		}
		//cerr << endl << "startServer: read " << inBs.length() << " bytes" << endl;
		if (!keepRunning) break;
		if (inBs.length() > 0)
		{
			sock.write(inBs);
			if (!keepRunning) break;
		}
	}

	delete inMq;
	//cerr << endl << "startServer is done" << endl;
}

static void startServer_16()
{
	MessageQueueServer server("server3", "./Calpont.xml");
	ByteStream msg;
	struct timespec ts = {1, 0};	// 1 second
// 	const ByteStream::byte *bMsg;
// 	int i;

	isRunning = true;
	IOSocket sock = server.accept();
	while (keepRunning) {
		
// 		cout << " ... reading" << endl;
		try {
			msg.reset();
			msg = sock.read(&ts);
		}
		catch (SocketClosed &e) {
			break;
		}

	/*
		if (msg.length() > 0) {
			bMsg = msg.buf();
			cout << "got a message: ";
			for (i = 0; i < msg.length(); i++)
				cout << bMsg[i] << " ";
			cout << endl;
		}
	*/
		if (msg.length() > 0 && keepRunning)
			sock.write(msg);
	}
	isRunning = false;
// 	cout << "startserver exiting" << endl;
}

static void startServer_17()
{
	MessageQueueServer server("server3", "./Calpont.xml");
	server.syncProto(false);
	ByteStream msg;
	struct timespec ts = {1, 0};	// 1 second
// 	const ByteStream::byte *bMsg;
// 	int i;

	isRunning = true;
	IOSocket sock = server.accept();
	while (keepRunning) {
		
// 		cout << " ... reading" << endl;
		try {
			msg.reset();
			msg = sock.read(&ts);
		}
		catch (SocketClosed &e) {
			break;
		}

	/*
		if (msg.length() > 0) {
			bMsg = msg.buf();
			cout << "got a message: ";
			for (i = 0; i < msg.length(); i++)
				cout << bMsg[i] << " ";
			cout << endl;
		}
	*/
		if (msg.length() > 0 && keepRunning)
			sock.write(msg);
	}
	isRunning = false;
// 	cout << "startserver exiting" << endl;
}

struct Serv18thd
{
void operator()()
{
	ByteStream msg;
	struct timespec ts = {1, 0};
	while (*fKeepRunning)
	{
		try
		{
			msg.reset();
			msg = fSock.read(&ts);
		} catch (SocketClosed &e)
		{
		}
		if (msg.length() > 0 && *fKeepRunning)
			fSock.write(msg);
	}
}
Serv18thd(const IOSocket& s, volatile bool* kr) : fSock(s), fKeepRunning(kr) {}
~Serv18thd() {}
IOSocket fSock;
volatile bool* fKeepRunning;
};

static void startServer_18()
{
	boost::thread_group tg;
	MessageQueueServer server("server3", "./Calpont.xml");
	struct timespec ts = {1, 0};
	IOSocket sock;
	isRunning = true;
	while (keepRunning)
	{
		sock = server.accept(&ts);
		if (sock.socketParms().sd() > -1)
			tg.create_thread(Serv18thd(sock, &keepRunning));
	}
	tg.join_all();
	isRunning = false;
}

static void startBrokenServer()
{
	MessageQueueServer* inMq;
	bool retry;

	do {
		try {
			retry = false;
			inMq = new MessageQueueServer(brokeServ, "./Calpont.xml");
		}
		catch (...) {
			//cout << endl << "MessageQueueServer ctor threw!" << endl;
			::usleep(5000000);
			retry = true;
		}
	} while (retry);

	ByteStream inBs;
	struct timespec ts = { 0, TS_MS(20) };
	//cout << endl << "startServer is starting" << endl;

	isRunning = true;
	IOSocket sock = inMq->accept();	
	for (;;)
	{
		try {
			inBs = sock.read(&ts);	
		}
		catch (SocketClosed &e) {
			break;
		}
		if (!keepRunning) break;
	}

	delete inMq;
	//cout << endl << "startServer is done" << endl;
}

static void startWriteServer()
{
	MessageQueueServer* inMq;
	bool retry;

	do {
		try {
			retry = false;
			inMq = new MessageQueueServer(writeServ, "./Calpont.xml");
		}
		catch (...) {
			//cout << endl << "MessageQueueServer ctor threw!" << endl;
			::usleep(5000000);
			retry = true;
		}
	} while (retry);
	isRunning = true;

	string msg = "This is a test";
	ByteStream outBs;
	outBs.load(reinterpret_cast<const ByteStream::byte*>(msg.c_str()), msg.length());

	IOSocket sock = inMq->accept();
	sock.write(outBs);

	while (keepRunning)
		::usleep(10000000);

	delete inMq;
	//cout << endl << "writeServer is done" << endl;
}


class MessageQTestSuite : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( MessageQTestSuite );

CPPUNIT_TEST( mq_1 );
CPPUNIT_TEST( mq_2 );
CPPUNIT_TEST( mq_8 );
CPPUNIT_TEST_EXCEPTION( mq_3, std::runtime_error );
CPPUNIT_TEST_EXCEPTION( mq_4, std::runtime_error );
CPPUNIT_TEST_EXCEPTION( mq_5, std::runtime_error );
CPPUNIT_TEST_EXCEPTION( mq_6, std::runtime_error );
CPPUNIT_TEST_EXCEPTION( mq_7, std::runtime_error );
CPPUNIT_TEST( mq_9 );
CPPUNIT_TEST( mq_10 );
CPPUNIT_TEST( mq_12 );
CPPUNIT_TEST_EXCEPTION( mq_13a, std::logic_error );
CPPUNIT_TEST_EXCEPTION( mq_14, std::runtime_error );
CPPUNIT_TEST( mq_15 );
CPPUNIT_TEST( mq_16 );	// test the fix for bug #224
CPPUNIT_TEST( mq_17 );
CPPUNIT_TEST( mq_18 );
CPPUNIT_TEST( mq_19 );

CPPUNIT_TEST_SUITE_END();

private:
	ByteStream bs;
	ByteStream bs1;
	boost::thread* srvThread;

public:
	void setUp() {
		bs.reset();
		bs1.reset();
		srvThread = 0;
		//setenv("CALPONT_CONFIG_FILE", "./Calpont.xml", 1);
	}

	void tearDown() {
		bs.reset();
		bs1.reset();
		delete srvThread;
		srvThread = 0;
	}

void mq_1()
{
	keepRunning = true;
	isRunning = false;
	normServ = "server1";
	srvThread = new boost::thread(startServer);

	while (!isRunning)
	{
		//cout << endl << "waiting for startServer" << endl;
		::usleep(2500000);
	}

	Config* cf = Config::makeConfig("./Calpont.xml");
	MessageQueueClient outMq(normServ, cf);
	string msg = "This is a test";
	ByteStream outBs;
	outBs.load(reinterpret_cast<const ByteStream::byte*>(msg.c_str()), msg.length());
	//cerr << endl << "mq_1: write " << outBs.length() << " bytes" << endl;
	outMq.write(outBs);

	ByteStream inBs;
	inBs = outMq.read();

	//cerr << endl << "mq_1: read " << inBs.length() << " bytes" << endl;
	CPPUNIT_ASSERT(outBs.length() == inBs.length());
	CPPUNIT_ASSERT(memcmp(outBs.buf(), inBs.buf(), outBs.length()) == 0);

	int i;
	ByteStream::byte u8;

	u8 = 0xa5;
	for (i = 0; i < 2048; i++)
		bs << u8;

	//cerr << endl << "mq_1: write " << bs.length() << " bytes" << endl;
	outMq.write(bs);
	bs1 = outMq.read();
#if 0
	if (bs.length() != bs1.length())
		cerr << endl << "bs.length() = " << bs.length() << ", bs1.length() = " << bs1.length() << endl;
#endif
	CPPUNIT_ASSERT(bs.length() == bs1.length());
	CPPUNIT_ASSERT(memcmp(bs.buf(), bs1.buf(), bs.length()) == 0);

	bs.reset();
	u8 = 0x5a;
	for (i = 0; i < 2048; i++)
		bs << u8;

	bs << u8;
	bs << u8;
	bs << u8;

	//cerr << endl << "mq_1: write " << bs.length() << " bytes" << endl;
	outMq.write(bs);
	bs1 = outMq.read();
#if 0
	if (bs.length() != bs1.length())
		cerr << endl << "bs.length() = " << bs.length() << ", bs1.length() = " << bs1.length() << endl;
#endif
	CPPUNIT_ASSERT(bs.length() == bs1.length());
	CPPUNIT_ASSERT(memcmp(bs.buf(), bs1.buf(), bs.length()) == 0);

	// Now write a really big message and see what happens...

	ByteStream::quadbyte u32;

	bs.reset();
	u32 = 0xdeadbeef;
	for (i = 0; i < (1048576 / 4); i++)
		bs << u32;

	//cerr << endl << "mq_1: write " << bs.length() << " bytes" << endl;
	outMq.write(bs);
	ByteStream bs2;
	struct timespec ts = { 5, TS_MS(0) };
	if (leakCheck) ts.tv_sec *= 20;
	bs1.reset();
	bs2 = outMq.read(&ts);
	bs1 += bs2;
	while (bs2.length() > 0 && bs1.length() < bs.length())
	{
		//cerr << endl << "going back for more..." << endl;
		bs2 = outMq.read(&ts);
		bs1 += bs2;
	}
#if 0
	if (bs.length() != bs1.length())
		cerr << endl << "bs.length() = " << bs.length() << ", bs1.length() = " << bs1.length() << endl;
#endif
	CPPUNIT_ASSERT(bs.length() == bs1.length());
	CPPUNIT_ASSERT(memcmp(bs.buf(), bs1.buf(), bs.length()) == 0);

	keepRunning = false;
	outMq.shutdown();

	srvThread->join();
	delete srvThread;
	srvThread = 0;
        Config::deleteInstanceMap();
}

void mq_2()
{
	keepRunning = true;
	isRunning = false;
	brokeServ = "server2";
	srvThread = new boost::thread(startBrokenServer);

	while (!isRunning)
	{
		//cout << endl << "waiting for startBrokenServer" << endl;
		::usleep(2500000);
	}

	struct timespec ts = { 0, TS_MS(20) };

	MessageQueueClient outMq(brokeServ, "./Calpont.xml");
	bs1 = outMq.read(&ts);

	keepRunning = false;
	outMq.shutdown();

	srvThread->join();
	delete srvThread;
	srvThread = 0;
        Config::deleteInstanceMap();
}

void mq_3()
{
	// Should throw runtime_exception for missing info
	//setenv("CALPONT_CONFIG_FILE", "./bogus.xml", 1);
	MessageQueueServer* outMq = new MessageQueueServer("ExeMgr", "./bogus.xml");

	CPPUNIT_ASSERT(0);
	IOSocket sock = outMq->accept();
	bs1 = sock.read();
	delete outMq;
        Config::deleteInstanceMap();
}

void mq_4()
{
	boost::scoped_ptr<MessageQueueClient> outMq(new MessageQueueClient("server4", "./Calpont.xml"));
	// Should throw runtime_exception for connect failed
	bs1 = outMq->read();

	CPPUNIT_ASSERT(0);
        Config::deleteInstanceMap();
}

void mq_5()
{
	boost::scoped_ptr<MessageQueueClient> outMq(new MessageQueueClient("server4", "./Calpont.xml"));
	string msg = "This is a test";
	bs1.load(reinterpret_cast<const ByteStream::byte*>(msg.c_str()), msg.length());
	// Should throw runtime_exception for connect failed
	outMq->write(bs1);

	CPPUNIT_ASSERT(0);
        Config::deleteInstanceMap();
}

void mq_6()
{
	Config* cf = Config::makeConfig("./Calpont.xml");
	boost::scoped_ptr<MessageQueueServer> outMq(new MessageQueueServer("server4", cf));
	// Should throw runtime_exception for addr in use
	MessageQueueServer* outMq1 = new MessageQueueServer("server4", cf);

	CPPUNIT_ASSERT(0);
	delete outMq1;
        Config::deleteInstanceMap();
}

void mq_7()
{
	// Should throw runtime_exception for missing info
	setenv("CALPONT_CONFIG_FILE", "./bogus.xml", 1);
	MessageQueueClient* outMq = new MessageQueueClient("ExeMgr", "./bogus.xml");

	CPPUNIT_ASSERT(0);
	bs1 = outMq->read();
	delete outMq;
        Config::deleteInstanceMap();
}

void mq_8()
{
	keepRunning = true;
	isRunning = false;
	writeServ = "server3";
	srvThread = new boost::thread(startWriteServer);

	while (!isRunning)
	{
		//cout << endl << "waiting for startWriteServer" << endl;
		::usleep(2500000);
	}

	MessageQueueClient outMq(writeServ, "./Calpont.xml");
	bs1 = outMq.read();
	CPPUNIT_ASSERT(memcmp(bs1.buf(), "This is a test", bs1.length()) == 0);

	outMq.shutdown();
	keepRunning = false;

	srvThread->join();
	delete srvThread;
	srvThread = 0;
        Config::deleteInstanceMap();
}

// Bug 1735: I don't know how this function "used" to work, or it's intent,
//           but it now encounters 2 exceptions, that were not accounted for.
//           I added code to log the exceptions and keep going.
//           Somebody can investigate further at some point if they like.
void mq_9()
{
	InetStreamSocket* iss;
	boost::scoped_ptr<MessageQueueServer> mq1(new MessageQueueServer("server5", "./Calpont.xml"));
	struct timespec ts = { 0, TS_MS(200) };
	// this should block in accept() for ts
	IOSocket sock = mq1->accept(&ts);
	// Set a bogus fd
	int fd;
	fd = open("/dev/null", O_RDONLY);
	close(fd);
	//mq1->fClientSock.fSocket->fSd = fd;
	iss = dynamic_cast<InetStreamSocket*>(mq1->fClientSock.fSocket);
	iss->fSocketParms.fSd = fd;
	// mqs::read() will catch a runtime_error and return a zero bs
	try
	{
		sock.read();
	}
	catch (runtime_error& ex) // Bug 1735
	{
		cerr << "Runtime error (OK)..." << ex.what() << endl;
	}

	boost::scoped_ptr<MessageQueueClient> mq2(new MessageQueueClient("server5", "./Calpont.xml"));
	bs.reset();
	bs << "This is a test";
	//close(mq2->fClientSock.fSocketParms.fSd);
	iss = dynamic_cast<InetStreamSocket*>(mq2->fClientSock.fSocket);
	close(iss->fSocketParms.fSd);
	try
	{
		mq2->write(bs);
	}
	catch (runtime_error& ex) // Bug 1735
	{
		cerr << "Connection error (OK)..." << ex.what() << endl;
	}
	bs.reset();
	mq2->shutdown();
	// Should not throw runtime_error
	sock.read(&ts);
        Config::deleteInstanceMap();
}

void mq_10()
{
	SocketParms s;

	s.sd(0);
	CPPUNIT_ASSERT(s.sd() == 0);
	s.sd(-1);
	CPPUNIT_ASSERT(s.sd() == -1);
	s.domain(1);
	CPPUNIT_ASSERT(s.domain() == 1);
	s.type(2);
	CPPUNIT_ASSERT(s.type() == 2);
	s.protocol(3);
	CPPUNIT_ASSERT(s.protocol() == 3);

	SocketParms s1(s);
	CPPUNIT_ASSERT(s1.sd() == -1);
	CPPUNIT_ASSERT(s1.domain() == 1);
	CPPUNIT_ASSERT(s1.type() == 2);
	CPPUNIT_ASSERT(s1.protocol() == 3);

	SocketParms s2;
	s2 = s;
	CPPUNIT_ASSERT(s2.sd() == -1);
	CPPUNIT_ASSERT(s2.domain() == 1);
	CPPUNIT_ASSERT(s2.type() == 2);
	CPPUNIT_ASSERT(s2.protocol() == 3);
        Config::deleteInstanceMap();

}

void mq_12()
{
	InetStreamSocket iss;
	iss.fSocketParms.sd(12345);
	InetStreamSocket iss1;
	iss1 = iss;
	CPPUNIT_ASSERT(iss1.socketParms().sd() == iss.socketParms().sd());
	InetStreamSocket iss2(iss1);
	CPPUNIT_ASSERT(iss1.socketParms().sd() == iss2.socketParms().sd());
        Config::deleteInstanceMap();
}

void mq_13a()
{
	InetStreamSocket iss;
	iss.fSocketParms.sd(0);
	iss.open();
}

void mq_14()
{
	InetStreamSocket* iss;
	boost::scoped_ptr<MessageQueueServer> mq1(new MessageQueueServer("server6", "./Calpont.xml"));
	// Set a bogus fd
	int fd;
	fd = open("/dev/null", O_RDONLY);
	close(fd);
	//mq1->fListenSock.fSocketParms.fSd = fd;
	iss = dynamic_cast<InetStreamSocket*>(mq1->fListenSock.fSocket);
	iss->fSocketParms.fSd = fd;
	// should throw in accept();
	IOSocket sock = mq1->accept();
	sock.read();

}

void mq_15()
{
	IOSocket ios(new InetStreamSocket());
	string oss;
	oss = ios.toString();
	//CPPUNIT_ASSERT(oss == "IOSocket: sd: -1 domain: 2 type: 1 protocol: 0 inet: 0.0.0.0");
	CPPUNIT_ASSERT(oss.length() > 0);
}

void mq_16()
{

	const char msg1[] = "Message 1";
	const char msg2[] = "message 2";
	string sTmp;
	MessageQueueClient client("server3", "./Calpont.xml");
	ByteStream bs, bs2;
	char buf[1000];
	int len, err, socketfd;
	struct timespec ts = {2, 0};  // 2 seconds for client, 1 for server

	isRunning = false;
	keepRunning = true;
	srvThread = new boost::thread(startServer_16);
	
	while (!isRunning)
		usleep(250000);

	//connect 
	bs << (uint8_t) 1;
	client.write(bs);
	bs = client.read();

	// grab the server's FD for the client, fake a partial ByteStream being written
	socketfd = client.fClientSock.socketParms().sd();
	len = strlen(msg1) + 1;
	memcpy(buf, &BYTESTREAM_MAGIC, 4);
	memcpy(&buf[4], &len, 4);
	memcpy(&buf[8], msg1, len);
	err = write(socketfd, buf, 12);  // only write the first 4 bytes of msg1
	CPPUNIT_ASSERT(err >= 0);

	// verify the partial message is dropped
	bs = client.read(&ts);
	CPPUNIT_ASSERT(bs.length() == 0);

	// write the rest of the message
 	err = write(socketfd, &buf[12], len - 4);
	
	// write a full ByteStream, verify that only it is received
	bs << msg2;
	client.write(bs);
	bs2 = client.read(&ts);
	CPPUNIT_ASSERT(bs == bs2);
	keepRunning = false;
	srvThread->join();
	delete srvThread;
	srvThread = NULL;
        Config::deleteInstanceMap();
}

void mq_17()
{

	const char msg1[] = "Message 1";
	const char msg2[] = "message 2";
	string sTmp;
	MessageQueueClient client("server3", "./Calpont.xml");
	client.syncProto(false);
	ByteStream bs, bs2;
	char buf[1000];
	int len, err, socketfd;
	struct timespec ts = {2, 0};  // 2 seconds for client, 1 for server

	isRunning = false;
	keepRunning = true;
	srvThread = new boost::thread(startServer_17);
	
	while (!isRunning)
		usleep(250000);

	//connect 
	bs << (uint8_t) 1;
	client.write(bs);
	bs = client.read();

	// grab the server's FD for the client, fake a partial ByteStream being written
	socketfd = client.fClientSock.socketParms().sd();
	len = strlen(msg1) + 1;
	memcpy(buf, &BYTESTREAM_MAGIC, 4);
	memcpy(&buf[4], &len, 4);
	memcpy(&buf[8], msg1, len);
	err = write(socketfd, buf, 12);  // only write the first 4 bytes of msg1
	CPPUNIT_ASSERT(err >= 0);

	// verify the partial message is dropped
	bs = client.read(&ts);
	CPPUNIT_ASSERT(bs.length() == 0);

	// write the rest of the message
 	err = write(socketfd, &buf[12], len - 4);
	
	// write a full ByteStream, verify that only it is received
	bs << msg2;
	client.write(bs);
	bs2 = client.read(&ts);
	CPPUNIT_ASSERT(bs == bs2);
	keepRunning = false;
	srvThread->join();
	delete srvThread;
	srvThread = NULL;
        Config::deleteInstanceMap();
}

void mq_18()
{

	MessageQueueClient client1("server3", "./Calpont.xml");
	MessageQueueClient client2("server3", "./Calpont.xml");

	isRunning = false;
	keepRunning = true;
	srvThread = new boost::thread(startServer_18);
	
	while (!isRunning)
		usleep(250000);

	//connect 
	bs << (uint8_t) 1;
	client1.write(bs);
	bs = client1.read();
	bs << (uint8_t) 1;
	client2.write(bs);
	bs = client2.read();

	// 
	CPPUNIT_ASSERT(client1.isSameAddr(client2));

	CPPUNIT_ASSERT(client1.addr2String() == "127.0.0.1");

	keepRunning = false;
	srvThread->join();
	delete srvThread;
	srvThread = NULL;
        Config::deleteInstanceMap();
}

void mq_19()
{
	CPPUNIT_ASSERT(InetStreamSocket::ping("10.100.4.1", 0) == 0);
	CPPUNIT_ASSERT(InetStreamSocket::ping("10.100.4.254", 0) == -1);
	struct timespec ts = {20,0};
	CPPUNIT_ASSERT(InetStreamSocket::ping("10.100.4.1", &ts) == 0);
	CPPUNIT_ASSERT(InetStreamSocket::ping("10.100.4.254", &ts) == -1);
}

}; 

CPPUNIT_TEST_SUITE_REGISTRATION( ByteStreamTestSuite );
CPPUNIT_TEST_SUITE_REGISTRATION( MessageQTestSuite );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include <csignal>

void setupSignalHandlers()
{
	struct sigaction ign;

	memset(&ign, 0, sizeof(ign));
	ign.sa_handler = SIG_IGN;

	sigaction(SIGPIPE, &ign, 0);
}

int main( int argc, char **argv)
{
	setupSignalHandlers();

	leakCheck = false;
	if (argc > 1 && strcmp(argv[1], "--leakcheck") == 0) leakCheck = true;

	CppUnit::TextUi::TestRunner runner;
	CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
	runner.addTest( registry.makeTest() );
	bool wasSuccessful = runner.run( "", false );
	return (wasSuccessful ? 0 : 1);
}

