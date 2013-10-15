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

// $Id: mainC.cpp 2101 2013-01-21 14:12:52Z rdempsey $

#include <iostream>
#include <string>
#include <cassert>
using namespace std;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "consts.h"

int main(int argc, char** argv)
{
	//MessageQueueClient client("NetServer", "./Calpont.xml");
	MessageQueueClient* client = 0;
	ByteStream obs;
	ByteStream ibs;
	unsigned int u;
	unsigned int i;
	unsigned int n;
	ByteStream::octbyte rc;
	ByteStream::octbyte bytes_written = 0;
	ByteStream::octbyte w;
	ByteStream::octbyte p;
	ByteStream psbs;

	srand(time(0));

	// how many blockops to pack into a message
	n = 16;
	if (argc > 2)
		n = atoi(argv[2]);

	for (u = 0; u < (PrimSize * n / sizeof(ByteStream::octbyte) - 1); u++)
	{
		rc = rand();
		obs << rc;
	}

	// how many messages to send (total blockops sent = n * u)
	u = 250000;
	if (argc > 1)
		u = atoi(argv[1]);

	p = obs.length();
	psbs << p;

/*
With n = 16, u = 250000, I get 54 Kblockops/s w/ 2 clients & 1 server
*/

	w = psbs.length() + obs.length();
	idbassert(w == (PrimSize * n));

	client = new MessageQueueClient("NetServer", "./Calpont.xml");

	for (i = 0; i < u; i++)
	{
		client->write(psbs);
		client->write(obs);
		bytes_written += w;
		ibs = client->read();
		ibs >> rc;
		if (p != rc) cerr << "wrote " << p << " bytes, server acked " << rc << " bytes" << endl;
	}

	psbs.reset();
	rc = 0;
	psbs << rc;
	client->write(psbs);
	bytes_written += psbs.length();
	ibs = client->read();
	ibs >> rc;
	idbassert(rc == 0);

	delete client;

	cout << "wrote " << (bytes_written / 1024) << " KB" << endl;

	return 0;
}

