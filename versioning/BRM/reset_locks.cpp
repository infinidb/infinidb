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

// $Id: reset_locks.cpp 1823 2013-01-21 14:13:09Z rdempsey $
//
#include <unistd.h>
#include <iostream>
using namespace std;

#include "calpontsystemcatalog.h"
#include "sessionmanager.h"
#include "mastersegmenttable.h"

#include "rwlock.h"
using namespace rwlock;

#include "shmkeys.h"

using namespace BRM;

#define RESET(str, i) \
	do { cout << " - checking " << str << " semaphore" << endl; \
	rwlock[i]->reset(); } while (0)

#define PRINT(str, i) \
	do { cout << " - checking " << str << " semaphore" << endl \
	<< "   - r: " << rwlock[i]->getReading() << " w: " \
	<< rwlock[i]->getWriting() << " rwt: " << rwlock[i]->getReadersWaiting() \
	<< " wwt: " << rwlock[i]->getWritersWaiting() << endl; } while (0)

namespace
{
bool vFlg;
bool nFlg;

void usage()
{
	cout << "usage: reset_locks [-vnh]" << endl;
	cout << "   reset all InfiniDB shared memory locks" << endl;
	cout << "   -h display this help" << endl;
	cout << "   -v verbose output" << endl;
	cout << "   -n don't actually reset anything (implies -v)" << endl;
}
}

int main(int argc, char** argv)
{
#ifdef _MSC_VER
	const char* envp = getenv("SystemRoot");
	string SystemRoot;
	if (envp && *envp)
		SystemRoot = envp;
	else
		SystemRoot = "C:\\WINDOWS";
	string tmpEnv = "TMP=" + SystemRoot + "\\Temp";
	_putenv(tmpEnv.c_str());
#endif

	BRM::DBRM dbrm;

	int c;
	opterr = 0;
	vFlg = false;
	nFlg = false;

	while ((c = getopt(argc, argv, "vnh")) != EOF)
		switch (c)
		{
		case 'v':
			vFlg = true;
			break;
		case 'n':
			nFlg = true;
			break;
		case 'h':
		default:
			usage();
			return (c == 'h' ? 0 : 1);
			break;
		}

	if (nFlg)
		vFlg = true;

	ShmKeys keys;

	cerr << "(Exception msgs are probably OK)" << std::endl;

	RWLock *rwlock[MasterSegmentTable::nTables];
	int RWLockKeys[MasterSegmentTable::nTables];
	int i;

	if (MasterSegmentTable::nTables != 5) {
		cout << "There are more locks than reset_locks knows of.  Time for an update." << endl;
		exit(1);
	}

	RWLockKeys[0] = keys.KEYRANGE_EXTENTMAP_BASE;
	RWLockKeys[1] = keys.KEYRANGE_EMFREELIST_BASE;
	RWLockKeys[2] = keys.KEYRANGE_VBBM_BASE;
	RWLockKeys[3] = keys.KEYRANGE_VSS_BASE;
	RWLockKeys[4] = keys.KEYRANGE_CL_BASE;

	for (i = 0; i < MasterSegmentTable::nTables; i++)
		rwlock[i] = new RWLock(RWLockKeys[i]);

	if (vFlg)
	{
		PRINT("Extent Map", 0);
		PRINT("Extent Map free list", 1);
		PRINT("VBBM", 2);
		PRINT("VSS", 3);
		PRINT("CL", 4);
	}

	if (!nFlg)
	{
		RESET("Extent Map", 0);
		RESET("Extent Map free list", 1);
		RESET("VBBM", 2);
		RESET("VSS", 3);
		RESET("CL", 4);

		if (dbrm.isDBRMReady())
		{
			cout << " - resetting SessionManager semaphore" << endl;
			try {
				execplan::SessionManager sm;
				sm.reset();
			}
			catch (std::exception &e) {
				std::cout << e.what() << std::endl;
			}
		}
	}

	for (i = 0; i < MasterSegmentTable::nTables; i++)
		delete rwlock[i];
		
	std::cout << "OK." << std::endl;
	return 0;
}

