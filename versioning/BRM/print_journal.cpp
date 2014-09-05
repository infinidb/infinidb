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
 * $Id: print_journal.cpp 880 2010-1-26 16:16:44Z pleblanc $
 *
 ****************************************************************************/

#include <iostream>
#include <string>
#include <cassert>
using namespace std;

#include "brm.h"
#include "slavecomm.h"
using namespace BRM;

#include "configcpp.h"
using namespace config;

void usage(char *name)
{
	cout << "Usage: " << name << " <prefix>" << endl;
	exit(1);
}

int main(int argc, char **argv)
{
	SlaveComm brm;
	int err;
	string prefix;

	Config* cf = Config::makeConfig();

	if (argc > 2)
		usage(argv[0]);
	else if (argc == 2)
		prefix = argv[1];
	else
		prefix = cf->getConfig("SystemConfig", "DBRMRoot");

	assert(!prefix.empty());

	err = brm.printJournal(prefix);
	if (err == -1) {
		cout << "Could not load BRM journal file" << endl;
		return 1;
	}

	cout << "done" << endl;
	return 0;
}
