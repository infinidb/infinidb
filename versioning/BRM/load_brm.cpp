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
 * $Id: load_brm.cpp 1756 2012-11-09 21:54:39Z rdempsey $
 *
 ****************************************************************************/
#include <iostream>
#include <string>
#include <unistd.h>
using namespace std;

#include <boost/interprocess/shared_memory_object.hpp>
namespace bi=boost::interprocess;

#include "blockresolutionmanager.h"
#include "shmkeys.h"
using namespace BRM;

namespace
{

void usage()
{
	cout << "usage: load_brm [-fh] prefix" << endl << endl;
	cout << "   -h display this help" << endl;
	cout << "   -f possibly fix a corrupted Free List" << endl;
}

}

int main(int argc, char **argv)
{
	opterr = 0;
	bool fflg = false;

	int c;
	while ((c = getopt(argc, argv, "fh")) != EOF)
		switch (c)
		{
		case 'f':
			fflg = true;
			break;
		case 'h':
		case '?':
		default:
			usage();
			return (c == 'h' ? 0 : 1);
			break;
		}

	if ((argc - optind) != 1)
	{
		usage();
		return 1;
	}

	BlockResolutionManager brm;
	int err;
	string prefix;

	prefix = argv[optind];

	err = brm.loadState(prefix, fflg);
	if (err != 0) {
		cout << "Loading BRM snapshot failed (" << prefix << ")\n";
		return 1;
	}

	err = brm.replayJournal(prefix);
	if (err < 0) {
		cout << "Could not load BRM journal file\n";
		return 1;
	}

	ShmKeys shmkeys;
	string key_name = ShmKeys::keyToName(shmkeys.DECOMSVRMUTEX_SYSVKEY);
	bi::shared_memory_object::remove(key_name.c_str());

	/* An OAM friendly success msg */
	cout << "OK.\n";
	cout << "Successfully loaded BRM snapshot\n";
	cout << "Successfully replayed " << err << " BRM transactions\n";

	return 0;
}
// vim:ts=4 sw=4:

