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
 * $Id: load_brm64.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 ****************************************************************************/

/*
 * Loads state of the BRM data structures from file.
 *
 * More detailed description
 */

#include "brmtypes.h"
#include "rwlock.h"
#include "mastersegmenttable.h"
#include "extentmap.h"
#include "copylocks.h"
#include "vss.h"
#include "vbbm.h"
#include "blockresolutionmanager.h"
#include "IDBPolicy.h"
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
#if __WORDSIZE <= 32
//This code is OBE now that the structs are padded correctly
	BlockResolutionManager brm;
	int err;
	string prefix;

	if (argc > 2)
		usage(argv[0]);
	else if (argc == 2)
		prefix = argv[1];
	else
		prefix = "BRM_state";

	idbdatafile::IDBPolicy::configIDBPolicy();

	err = brm.loadState(prefix);
	if (err == 0)
		cout << "OK." << endl;
	else {
		cout << "Load failed" << endl;
		return 1;
	}

	return 0;
#else
	cerr << "This tool does not work on 64-bit arch!" << endl;
	return 1;
#endif
}
