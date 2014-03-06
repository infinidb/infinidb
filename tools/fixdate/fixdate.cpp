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
* $Id: fixdate.cpp 2101 2013-01-21 14:12:52Z rdempsey $
*
*/

#include <iostream>
#include <fstream>
#include <cassert>
#include <vector>
#include <algorithm>
#include <iterator>
using namespace std;

#include <unistd.h>

#include <boost/format.hpp>
#include <boost/scoped_array.hpp>
using namespace boost;

#include "bytestream.h"
using namespace messageqcpp;

#include "dmlpackageprocessor.h"
using namespace dmlpackageprocessor;

namespace {

const streamsize blkSz = 8192;

void usage()
{
}

u_int64_t fixDate(ByteStream& bs, ostream& out)
{
	ByteStream fixed;
	ByteStream::quadbyte o;
	u_int64_t cnt = 0;
#if 0
	DMLPackageProcessor::Date minDate;
	DMLPackageProcessor::Date maxDate;
	minDate.year = 1992;
	minDate.month = 1;
	minDate.day = 2;
	maxDate.year = 1998;
	maxDate.month = 12;
	maxDate.day = 25;
	ByteStream::quadbyte mxd;
	ByteStream::quadbyte mnd;
	mxd = *(reinterpret_cast<ByteStream::quadbyte*>(&maxDate));
	mnd = *(reinterpret_cast<ByteStream::quadbyte*>(&minDate));
#endif
	DMLPackageProcessor::Date fixDate;
	fixDate.spare = 0;
	ByteStream::quadbyte f;
	while (bs.length() > 0)
	{
		bs >> o;
		if (o >= 0xfffffffe)
		{
			fixed << o;
			continue;
		}
		f = o & 0xffff;
		fixDate.year = f;
		o >>= 16;
		f = o & 0xf;
		fixDate.month = f;
		o >>= 4;
		f = o & 0x3f;
		fixDate.day = f;
		//o >>= 6;
		o = *(reinterpret_cast<ByteStream::quadbyte*>(&fixDate));
		fixed << o;
#if 0
		cout << DMLPackageProcessor::dateToString(o) << endl;
		idbassert(o >= mnd && o <= mxd);
		cnt++;
#endif
	}
	out << fixed;
	return cnt;
}

}

int main(int argc, char* argv[])
{
	int c;

	opterr = 0;

	while ((c = getopt(argc, argv, "h")) != EOF)
		switch (c)
		{
		case 'h':
			usage();
			return 0;
			break;
		default:
			usage();
			return 1;
			break;
		}

	if ((argc - optind) < 1)
	{
		usage();
		return 1;
	}

	ByteStream bs;

	ifstream ifs(argv[optind + 0]);
	ByteStream::byte inbuf[blkSz];
	streampos fLen;
	u_int64_t blkNo = 0;

	ifs.seekg(0, ios_base::end);
	fLen = ifs.tellg();
	ifs.seekg(0, ios_base::beg);

	idbassert((fLen % blkSz) == 0);
	u_int64_t numBlks = fLen / blkSz;
	cout << numBlks << " blocks to fix..." << endl;

	ofstream ofs("fixdate.cdf");

	cout << "pct done:    " << setw(3);

	for(;;)
	{

		ifs.read(reinterpret_cast<char*>(inbuf), blkSz);
		if (ifs.eof()) break;
		bs.load(inbuf, blkSz);

		fixDate(bs, ofs);
		cout << "\b\b\b" << setw(3) << (u_int64_t)(blkNo * 100 / numBlks);
		//cout << setw(3) << (u_int64_t)(blkNo * 100 / numBlks) << endl;

		blkNo++;
	}

	cout << "\b\b\b" << setw(3) << 100 << endl;
	//cout << setw(3) << 100 << endl;

	return 0;
}

