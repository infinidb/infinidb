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
			"   which_lock_to_use: 1=VSS 2=ExtentMap 3=FreeList 4=VBBM 5=CopyLocks\n" <<
			"   which_side_to_use: r|w   (read or write)\n" <<
			"   lock_or_unlock:    l|u   (lock or unlock)\n";
	exit(1);
}

int main(int argc, char **argv)
{
	uint32_t which_lock;  // 1-5
	uint32_t which_side;  // 0 or 1
	uint32_t lock_unlock;  // 0 or 1
	RWLock *rwlock;

	name = argv[0];

	if (argc != 4)
		usage();
	if (strlen(argv[1]) != 1 || strlen(argv[2]) != 1 || strlen(argv[3]) != 1)
		usage();

	which_lock = atoi(argv[1]);
	if (which_lock < 1 || which_lock > 5)
		usage();

	if (argv[2][0] == 'r')
		which_side = 0;
	else if (argv[2][0] == 'w')
		which_side = 1;
	else
		usage();

	if (argv[3][0] == 'l')
		lock_unlock = 0;
	else if (argv[3][0] == 'u')
		lock_unlock = 1;
	else
		usage();

	rwlock = new RWLock(0x10000 * which_lock);
	if (which_side == 0)
		if (lock_unlock == 0)
			rwlock->read_lock();
		else
			rwlock->read_unlock();
	else
		if (lock_unlock == 0)
			rwlock->write_lock();
		else
			rwlock->write_unlock();

	return 0;
}
