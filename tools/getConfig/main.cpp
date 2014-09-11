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

/*****************************************************************************
* $Id: main.cpp 1397 2011-02-03 17:56:12Z rdempsey $
*
*****************************************************************************/
#include <iostream>
#include <cassert>
#include <unistd.h>
using namespace std;

#include "configcpp.h"
using namespace config;

namespace
{

void usage(const string& pname)
{
	cout << "usage: " << pname << " [-vh] [-c config_file] section param" << endl <<
		"   Displays configuration variable param from section section." << endl <<
		"   -c config_file use config file config_file" << endl <<
		"   -v display verbose information" << endl <<
		"   -h display this help text" << endl;
}

}

int main(int argc, char** argv)
{
	int c;
	string pname(argv[0]);
	bool vflg = false;
	string configFile;

	opterr = 0;

	while ((c = getopt(argc, argv, "c:vh")) != EOF)
		switch (c)
		{
		case 'v':
			vflg = true;
			break;
		case 'c':
			configFile = optarg;
			break;
		case 'h':
		case '?':
		default:
			usage(pname);
			return (c == 'h' ? 0 : 1);
			break;
		}

	if ((argc - optind) < 2)
	{
		usage(pname);
		return 1;
	}

	const Config* cf;
	if (configFile.length() > 0)
		cf = Config::makeConfig(configFile);
	else
		cf = Config::makeConfig();

	if (vflg)
	{
		cout << "Using config file: " << cf->configFile() << endl;
	}

	cout << cf->getConfig(argv[optind + 0], argv[optind + 1]) << endl;

	return 0;
}

