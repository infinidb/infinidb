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
 * $Id: load_em.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 ****************************************************************************/

/*
 * Loads state of the BRM data structures from file.
 *
 * More detailed description
 */

#include "IDBPolicy.h"
#include "brmtypes.h"
#include "rwlock.h"
#include "mastersegmenttable.h"
#include "extentmap.h"

#include <iostream>

using namespace BRM;
using namespace std;

void usage(char *name)
{
	cout << "Usage: " << name << " <prefix>" << endl;
	exit(1);
}

int main(int argc, char **argv)
{
	ExtentMap em;
	string prefix;

	if (argc > 2)
		usage(argv[0]);
	else if (argc == 2)
		prefix = argv[1];
	else
		prefix = "BRM_state";

	idbdatafile::IDBPolicy::configIDBPolicy();

	try {
		em.load(prefix);
		cout << "OK." << endl;
	}
	catch (exception &e) {
		cout << "Load failed." << endl;
		return 1;
	}

	return 0;
}
