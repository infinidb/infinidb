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
 * $Id: save_brm.cpp 1266 2011-02-08 14:36:09Z rdempsey $
 *
 ****************************************************************************/

/*
 * Saves the current state of the BRM data structures.
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
#include <iostream>
#include <fstream>
#include "configcpp.h"

#include <sys/types.h>
#include <sys/stat.h>

using namespace BRM;
using namespace std;

int main (int argc, char **argv)
{
	BlockResolutionManager brm;
	config::Config *config = config::Config::makeConfig();
	int err;
	string prefix, currentFilename;
	ofstream currentFile;

	if (argc > 1)
		prefix = argv[1];
	else {
		prefix = config->getConfig("SystemConfig", "DBRMRoot");
		if (prefix.length() == 0) {
			cerr << "Error: Need a valid Calpont configuation file" << endl;
			exit(1);
		}
	}

	err = brm.saveState(prefix);
	if (err == 0)
		cout << "Saved to " << prefix << endl;
	else {
		cout << "Save failed" << endl;
		exit(1);
	}
#ifndef _MSC_VER
	mode_t utmp;
	utmp = ::umask(0);
#endif
	currentFilename = prefix + "_current";
	currentFile.open(currentFilename.c_str(), ios_base::trunc);
	if (!currentFile) {
		cerr << "Error: could not open " << currentFilename << "for writing" << endl;
		exit(1);
	}
	try {
		currentFile << prefix << endl;
	}
	catch (exception &e) {
		cerr << "Error: failed to write to " << currentFilename << ": " << e.what() << endl;
		exit(1);
	}
	try {
		currentFile.close();
	}
	catch (exception &e) {
		cerr << "Error: failed to close " << currentFilename << ": " << e.what() << endl;
		exit(1);
	}

#ifndef _MSC_VER
	utmp = ::umask(0);
#endif

	return 0;
}
