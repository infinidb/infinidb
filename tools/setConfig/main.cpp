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
* $Id: main.cpp 210 2007-06-08 17:08:26Z rdempsey $
*
*****************************************************************************/
#include <unistd.h>
#include <iostream>
#include <cassert>
#include <stdexcept>
using namespace std;

#include "configcpp.h"
using namespace config;

#include "liboamcpp.h"
using namespace oam;

namespace
{

void usage(const string& pname)
{
	cout << "usage: " << pname << " [-vdhx] [-c config_file] section param value" << endl <<
		"   Updates configuration variable param in section section with value." << endl <<
		"   -c config_file use config file config_file" << endl <<
		"   -v display verbose information" << endl <<
		"   -d don't perform misc checks and don't try to distribute the config file" << endl <<
		"      after changes are made" << endl <<
		"   -x delete the param from section (value is still required but ignored)" << endl <<
		"   -h display this help text" << endl;
}

}

int main(int argc, char** argv)
{
	int c;
	string pname(argv[0]);
	bool vflg = false;
	bool dflg = false;
	bool xflg = false;
	string configFile;

	opterr = 0;

	while ((c = getopt(argc, argv, "c:vdxh")) != EOF)
		switch (c)
		{
		case 'v':
			vflg = true;
			break;
		case 'd':
			dflg = true;
			break;
		case 'c':
			configFile = optarg;
			break;
		case 'x':
			xflg = true;
			break;
		case 'h':
		case '?':
		default:
			usage(pname);
			return (c == 'h' ? 0 : 1);
			break;
		}

	if ((argc - optind) < 3)
	{
		usage(pname);
		return 1;
	}

#ifdef COMMUNITY_KEYRANGE
	//No OAM in CE...
	dflg = true;
#endif

	Oam oam;
	oamModuleInfo_t t;
	bool parentOAMModuleFlag = true;
	string parentOAMModule = " ";
	int serverInstallType = oam::INSTALL_COMBINE_DM_UM_PM;

	//get local module info; validate running on Active Parent OAM Module
	try {
		t = oam.getModuleInfo();
		parentOAMModuleFlag = boost::get<4>(t);
		parentOAMModule = boost::get<3>(t);
		serverInstallType = boost::get<5>(t);
	}
	catch (exception&) {
		parentOAMModuleFlag = true;
	}

	if (!dflg && !parentOAMModuleFlag)
	{
		cerr << "Exiting, setConfig can only be run on the Active "
			"OAM Parent Module '" << parentOAMModule << "'" << endl;
		return 2;
	}

	Config* cf;
	if (configFile.length() > 0)
		cf = Config::makeConfig(configFile);
	else
		cf = Config::makeConfig();

	if (vflg)
		cout << "Using config file: " << cf->configFile() << endl;

	if (xflg)
		cf->delConfig(argv[optind + 0], argv[optind + 1]);
	else
		cf->setConfig(argv[optind + 0], argv[optind + 1], argv[optind + 2]);
	cf->write();

	if (dflg || serverInstallType == oam::INSTALL_COMBINE_DM_UM_PM)
		return 0;

	//get number of pms
	string count = cf->getConfig("PrimitiveServers", "Count");

	try {
		oam.distributeConfigFile();
		//sleep to give time for change to be distributed
		sleep(atoi(count.c_str()));
	}
	catch (...) {
		return 1;
	}

	return 0;
}
// vim:ts=4 sw=4:

