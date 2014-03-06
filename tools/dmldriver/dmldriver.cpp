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

// $Id: dmldriver.cpp 2101 2013-01-21 14:12:52Z rdempsey $

//#define NDEBUG
#include <cassert>
#include <iostream>
#include <string>
#include <unistd.h>
#include <cctype>
#include <sstream>
#include <fstream>
#include <limits>
using namespace std;

#include <boost/scoped_array.hpp>
#include <boost/tokenizer.hpp>
using namespace boost;

#include "tpchrf2.h"
#include "dmlif.h"

namespace
{
bool vflg;
bool dflg;

void usage()
{
	cout << "usage: dmldriver [-vhd] [-c intvl] [-f file] [-s sid] [-t flgs] [-r file] [-p cnt] [sql_text]" << endl;
	cout << "   -c intvl \tcommit every intvl statements" << endl;
	cout << "   -f file  \tread statements from file (max 15KB/stmt)" << endl;
	cout << "   -s sid   \tset sid as session id" << endl;
	cout << "   -t flgs  \tset trace flags" << endl;
	cout << "   -v       \tdisplay affected row count(s)" << endl;
	cout << "   -d       \tdisplay debug info" << endl;
	cout << "   -r file  \tread orderkeys from file for TPC-H RF2" << endl;
	cout << "   -p cnt   \tpack cnt orderkeys into each delete stmt (only w/ -r)" << endl;
	cout << "   -e schema\tset the schema name (only w/ -r)" << endl;
	cout << "   -h       \tdisplay this help text" << endl;
}
}

int main(int argc, char** argv)
{
	int c;

	opterr = 0;

	vflg = false;
	dflg = false;

	bool fflg = false;
	string infilename;

	int cIntvl = numeric_limits<int>::max();

	uint32_t sessionID = time(0) & 0x7fffffff;

	bool rflg = false;

	int packCnt = 1;

	uint32_t tflg = 0;

	string schema;

	while ((c = getopt(argc, argv, "e:t:s:c:f:r:p:vhd")) != EOF)
		switch (c)
		{
		case 'v':
			vflg = true;
			break;
		case 'd':
			dflg = true;
			break;
		case 'f':
			fflg = true;
			infilename = optarg;
			break;
		case 'c':
			cIntvl = static_cast<int>(strtol(optarg, 0, 0));
			break;
		case 's':
			sessionID = static_cast<uint32_t>(strtoul(optarg, 0, 0));
			break;
		case 't':
			tflg = static_cast<uint32_t>(strtoul(optarg, 0, 0));
			break;
		case 'r':
			rflg = true;
			infilename = optarg;
			break;
		case 'p':
			packCnt = static_cast<int>(strtol(optarg, 0, 0));
			break;
		case 'e':
			schema = optarg;
			break;
		case 'h':
		case '?':
		default:
			usage();
			return (c == 'h' ? 0 : 1);
			break;
		}

	if (!fflg && !rflg && ((argc - optind) < 1))
	{
		usage();
		return 1;
	}

	if (fflg && rflg)
	{
		cout << "-f and -r are mutually exclusive!" << endl << endl;
		usage();
		return 1;
	}

	if (!schema.empty() && !rflg)
	{
		cout << "-e requires -r!" << endl << endl;
		usage();
		return 1;
	}

	string stmtStr;
	if (!fflg && !rflg)
		stmtStr = argv[optind++];

	int rc = 0;

	dmlif::DMLIF dmlif(sessionID, tflg, dflg, vflg);

	if (fflg)
	{
		ifstream ifs(infilename.c_str());
		if (!ifs.good())
		{
			cerr << "Error accessing file " << infilename << endl;
			return 1;
		}
		const streamsize ilinelen = 15 * 1024;
		scoped_array<char> iline(new char[ilinelen]);
		int cnt = 0;
		for (;;)
		{
			ifs.getline(iline.get(), ilinelen);
			if (ifs.eof())
				break;
			rc = dmlif.sendOne(iline.get());
			if (rc != 0)
				break;
			cnt++;
			if ((cnt % cIntvl) == 0)
				dmlif.sendOne("COMMIT;");
		}
	}
	else if (rflg)
	{
		ifstream ifs(infilename.c_str());
		if (!ifs.good())
		{
			cerr << "Error accessing file " << infilename << endl;
			return 1;
		}
		if (schema.empty())
			schema = "tpch";
		tpch::RF2 rf2(schema, sessionID, tflg, cIntvl, packCnt, dflg, vflg);
		rc = rf2.run(ifs);
	}
	else
	{
		rc = dmlif.sendOne(stmtStr);
	}

	if (rc == 0)
		dmlif.sendOne("COMMIT;");

	return 0;
}

