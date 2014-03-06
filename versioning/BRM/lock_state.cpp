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

/* Takes two params,
 *    first, which lock use
 *    second, which side to use (read or write)
 *    third, lock or unlock it
 */

#include <iostream>
#include <stdlib.h>
#include <rwlock.h>

using namespace std;
using namespace rwlock;

char *name;

void usage()
{
	cout << "Usage " << name << " which_lock_to_use which_side_to_use lock_or_unlock\n" <<
			"   which_lock_to_use: 1=VSS 2=ExtentMap 3=FreeList 4=VBBM 5=CopyLocks\n";
	exit(1);
}

int main(int argc, char **argv)
{
	uint32_t which_lock;  // 1-5
	RWLock *rwlock;
	LockState state;

	name = argv[0];

	if (argc != 2)
		usage();
	if (strlen(argv[1]) != 1)
		usage();

	which_lock = atoi(argv[1]);
	if (which_lock < 1 || which_lock > 5)
		usage();

	rwlock = new RWLock(0x10000 * which_lock);
	state = rwlock->getLockState();
	cout << "readers = " << state.reading << endl
		 << "writers = " << state.writing << endl
		 << "readers waiting = " << state.readerswaiting << endl
		 << "writers waiting = " << state.writerswaiting << endl
		 << "mutex locked = " << (int) state.mutexLocked << endl;

	return 0;
}
