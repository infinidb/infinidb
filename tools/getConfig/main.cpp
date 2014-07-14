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
* $Id: main.cpp 2101 2013-01-21 14:12:52Z rdempsey $
*
*****************************************************************************/
#include <iostream>
#include <cassert>
#include <unistd.h>
using namespace std;

#include "logger.h"
#include "loggingid.h"
#include "messagelog.h"
#include <syslog.h>
#include <exception>
#include "configcpp.h"
using namespace config;

namespace
{

void usage(const string& pname)
{
	cout << "usage: " << pname << " [-vh] [-c config_file] section param" << endl <<
		"   Displays configuration variable param from section section." << endl <<
		"   -c config_file use config file config_file" << endl <<
		"   -a display all configuration values" << endl <<
		"   -i display all configuration values in .ini-file format (implies -a)" << endl <<
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
	bool aflg = false;
	bool iflg = false;

	opterr = 0;

	while ((c = getopt(argc, argv, "c:vaih")) != EOF)
		switch (c)
		{
		case 'v':
			vflg = true;
			break;
		case 'c':
			configFile = optarg;
			break;
		case 'a':
			aflg = true;
			break;
		case 'i':
			iflg = aflg = true;
			break;
		case 'h':
		case '?':
		default:
			usage(pname);
			return (c == 'h' ? 0 : 1);
			break;
		}

	if (!aflg && ( (argc - optind) < 2 ))
	{
		usage(pname);
		return 1;
	}

    try
    {
        openlog("getConfig", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_USER);
		Config* cf;
		if (configFile.length() > 0)
			cf = Config::makeConfig(configFile);
		else
			cf = Config::makeConfig();

		if (vflg)
		{
			cout << "Using config file: " << cf->configFile() << endl;
		}

		if (aflg)
		{
			vector<string> secs;
			vector<string> parms;
			secs = cf->enumConfig();
			vector<string>::iterator siter;
			vector<string>::iterator send;
			vector<string>::iterator piter;
			vector<string>::iterator pend;
			siter = secs.begin();
			send = secs.end();
			while (siter != send)
			{
				if (iflg)
					cout << '[' << *siter << ']' << endl;
				parms = cf->enumSection(*siter);
				piter = parms.begin();
				pend = parms.end();
				while (piter != pend)
				{
					if (iflg)
						cout << *piter << " = " << cf->getConfig(*siter, *piter) << endl;
					else
						cout << *siter << '.' << *piter << " = " <<
							cf->getConfig(*siter, *piter) << endl;
					++piter;
				}
				++siter;
				if (iflg)
					cout << endl;
			}
			return 0;
		}

		cout << cf->getConfig(argv[optind + 0], argv[optind + 1]) << endl;
        closelog();
    }
    catch(exception &e)
    {
        syslog(LOG_ERR, "Exception in getConfig for %s %s : %s", argv[optind + 0], argv[optind + 1], e.what());
        closelog(); 
    }
    catch(...)
    {
        syslog(LOG_ERR, "Exception in getConfig for %s %s : Unknown exception", argv[optind + 0], argv[optind + 1]);
        closelog();
	}
	return 0;
}

