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

#include <iostream>
#include <csignal>
#include <sstream>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
using namespace std;

#include <boost/thread.hpp>
using namespace boost;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

namespace
{

class MyRand
{
public:
	MyRand()
	{
		unsigned int seed = 0;
		int fd;
		//random will block, urandom will not
		fd = ::open("/dev/urandom", O_RDONLY);
		if (fd < 0)
			throw runtime_error("Could not access random device!");
		::read(fd, &seed, sizeof(seed));
		::close(fd);
		::srand(seed);
	}
	~MyRand() { }

	inline const int gen(const int min, const int max) const
	{
		return (min + (int)((float)(max - min + 1) * (float)(::rand() / (RAND_MAX + 1.0))));
	}
private:
	//MyRand(const MyRand& rhs);
	//MyRand& operator=(const MyRand& rhs);
};

void genRow(ByteStream* bsp, const MyRand& mr)
{
	const int rowlen = 1 + 12 + 1 + (6 * 4) + (6 * 22);
	char line[rowlen+1];
	char colstrs[6][22];
	char datestr[13];
	int i;
	float dateoff;

	memset(line, 0, rowlen+1);

	for (i = 0; i < 6; i++)
	{
		int64_t val;
		val = mr.gen(-15000, 15000);
		if (val > 13000)
			colstrs[i][0] = 0;
		else
			sprintf(colstrs[i], "%ld", val);
	}

	strcpy(colstrs[4], "1062625");	//cmpgn_nbr

	for (i = 0; i < 6; i++)
	{
		char colstrlen[5];
		unsigned len = strlen(colstrs[i]);
		idbassert(len <= 4000);
		sprintf(colstrlen, "%04u", len);
		strcat(line, colstrlen);
	}

	strcat(line, colstrs[0]);	//prim_cookie
	if (mr.gen(0, 1))
		strcat(line, "I");	//trans_typ
	else
		strcat(line, "V");
	strcat(line, colstrs[1]);	//site_nbr
	strcat(line, colstrs[2]);	//site_section_id
	strcat(line, colstrs[3]);	//creativeid
	strcat(line, colstrs[4]);	//cmpgn_nbr
	dateoff = mr.gen(0, 90) + (365 * 100 + 100 / 4 - 1) + (365 * 6 + 2);
	sprintf(datestr, "%012.6f", dateoff);
	strcat(line, datestr);		//record_timestamp
	strcat(line, colstrs[5]);	//revenue
	if (mr.gen(0, 1))
		strcat(line, "L");	//ping_type
	else
		strcat(line, "V");

	bsp->restart();
	bsp->load(reinterpret_cast<const uint8_t*>(line), strlen(line));
}

void exit_(int)
{
	exit(0);
}

void setupSignalHandlers()
{
	struct sigaction ign;

	memset(&ign, 0, sizeof(ign));
	ign.sa_handler = SIG_IGN;

	sigaction(SIGPIPE, &ign, 0);

	memset(&ign, 0, sizeof(ign));
	ign.sa_handler = exit_;

	sigaction(SIGUSR1, &ign, 0);
}

struct f
{
	f(const IOSocket& i, int lc=1000000) : ios(i), loopCnt(lc) { }

	void operator()()
	{
		MyRand mr;
		ByteStream bs;
		time_t start_time = time(0);
		cout << "Sending rows..." << endl;

		char tbhd[400];
		memset(tbhd, ' ', 400);

		memcpy(&tbhd[ 1 * 4], "0000", 4);	//prim_cookie i8
		memcpy(&tbhd[ 3 * 4], "0001", 4);	//trans_typ c1
		memcpy(&tbhd[11 * 4], "0000", 4);	//site_nbr i4
		memcpy(&tbhd[14 * 4], "0000", 4);	//site_section_id i4
		memcpy(&tbhd[23 * 4], "0000", 4);	//creativeid i4
		memcpy(&tbhd[34 * 4], "0000", 4);	//cmpgn_nbr i4
		memcpy(&tbhd[35 * 4], "0012", 4);	//record_timestamp date
		memcpy(&tbhd[37 * 4], "0000", 4);	//revenue i8
		memcpy(&tbhd[40 * 4], "0001", 4);	//ping_type c1

		bs.restart();
		bs.load(reinterpret_cast<const uint8_t*>(tbhd), 400);

		try {
			ios.write_raw(bs);
		} catch (...) {
			cout << "write() threw" << endl;
		}

#if 0
		const int rowlen = 1 + 12 + 1 + (6 * 4) + (6 * 4);
		char line[rowlen+1];
		memset(line, ' ', rowlen);
		strcpy(line, "0004" "0004" "0004" "0004" "0004" "0004");
		strcat(line, "1239");
		strcat(line, "I");
		strcat(line, "1679");
		strcat(line, "1019");
		strcat(line, "1459");
		strcat(line, "1899");
		strcat(line, "00001.000000");
		strcat(line, "1129");
		strcat(line, "L");

		bs.restart();
		bs.load(reinterpret_cast<const uint8_t*>(line), rowlen);
#endif

		for (int i = loopCnt; i > 0; --i)
		{
			genRow(&bs, mr);
			try {
				ios.write_raw(bs);
			} catch (...) {
				cout << "write() threw" << endl;
				break;
			}
		}

		time_t end_time = time(0);
		cout << "Done: " << (end_time - start_time) << endl;
		ios.close();
	}
	IOSocket ios;
	int loopCnt;
};

}

int main(int argc, char** argv)
{
	char *p;
	p = getenv("CALPONT_CONFIG_FILE");
	if (p == 0 || *p == 0) p = "./config.xml";
	string name("DataPump");
	string config(p);
	MessageQueueServer* mqs;
	IOSocket ios;
	thread* tp;
	int c;
	int loopCnt = 1000000;

	setupSignalHandlers();

	opterr = 0;

	while ((c = getopt(argc, argv, "l:")) != EOF)
		switch (c)
		{
		case 'l':
			loopCnt = atoi(optarg);
			break;
		default:
			break;
		}

	if (loopCnt <= 0)
	{
		cerr << "Bad loop count: " << loopCnt << endl;
		return 1;
	}

	mqs = new MessageQueueServer(name, config);

	for (;;)
	{
		ios = mqs->accept();
		cout << "New thread..." << endl;
		tp = new thread(f(ios, loopCnt));
		delete tp;
	}

	delete mqs;

	return 0;
}

