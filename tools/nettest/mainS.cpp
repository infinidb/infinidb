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

// $Id: mainS.cpp 2101 2013-01-21 14:12:52Z rdempsey $

#include <iostream>
#include <string>
#include <cassert>
#include <ctime>
using namespace std;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;
#include "threadpool.h"
using namespace threadpool;

#include "consts.h"

namespace {

ByteStream::octbyte process_block(const ByteStream& bs)
{
	const ByteStream::byte* b = bs.buf();
	const size_t l = bs.length() / sizeof(ByteStream::octbyte);
	const ByteStream::octbyte* o = reinterpret_cast<const ByteStream::octbyte*>(b);
	ByteStream::octbyte total = 0;
	for (size_t i = 0; i < l; i++, o++)
	{
		if (*o == 0xdeadbeefbadc0ffeLL)
			total++;
		if (*o == 0x1001LL)
			total++;
		if (*o == 0x2002LL)
			total++;
	}
	return total;
}

struct f
{
    IOSocket fIos;
    
    void operator()()
    {
		ByteStream ibs;
		ByteStream obs;
		ByteStream::octbyte bytes_read = 0;
		time_t start_time;
		time_t end_time;
		ByteStream::octbyte rc = 0;
		ByteStream::octbyte rd;
		ByteStream blockbs;

		for (unsigned int i = 0; i < 8192 / sizeof(ByteStream::octbyte); i++) blockbs << rc;
		idbassert(blockbs.length() == 8192);

		start_time = time(0);
		for (;;)
		{
			ibs.reset();
			ibs += fIos.read();
			ibs >> rd;
			while (ibs.length() < rd)
			{
				ibs += fIos.read();
			}
			rc = ibs.length();
			obs.reset();
			obs << rc;
			bytes_read += rc + sizeof(rd);
			fIos.write(obs);
			if (rd == 0) break;
			process_block(blockbs);
		}
		end_time = time(0);

		cout << "Read " << (bytes_read / 1024) << " KB in " << (end_time - start_time) << " seconds (" <<
			(bytes_read / (end_time - start_time) / 1024) << " KB/s)" << endl;
		cout << (bytes_read / (end_time - start_time) / PrimSize / 1024) << " Kblockops/s" << endl;
#ifdef PROFILE
		exit(0);
#endif

    }
};

}

int main(int argc, char* argv[])
{
    MessageQueueServer mqs("NetServer", "./Calpont.xml", 32768);
    ThreadPool tp(16, 80);
    f fo;
    
    for (;;)
    {
        fo.fIos = mqs.accept();
        tp.invoke(fo);
    }

    return 0;
}

