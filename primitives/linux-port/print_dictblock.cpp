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
 * $Id$
 *
 ****************************************************************************/

/** @file
 * Brief description of the file contents
 *
 * More detailed description
 */

#include <iostream>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "calpontsystemcatalog.h"

using namespace std;

void usage(char *name)
{
	cerr << "Usage: " << name << " dict_block_filename" << endl;
	exit(0);
}

void parseDictBlock(char *block) 
{

	uint16_t *offsets;
	uint16_t *freeBytes;
	u_int64_t *contPtr;
	int offsetIndex, size;
	char sig[BLOCK_SIZE+1];

	freeBytes = reinterpret_cast<uint16_t *>(&block[0]);
	contPtr = reinterpret_cast<u_int64_t *>(&block[2]);
	offsets = reinterpret_cast<uint16_t *>(&block[10]);

	cout << "Free Bytes: " << *freeBytes << endl;
	cout << "Continuation Pointer: 0x" << hex << *contPtr << dec << endl;
	
	for (offsetIndex = 0; offsets[offsetIndex+1] != 0xffff; offsetIndex++) {
		size = offsets[offsetIndex] - offsets[offsetIndex+1];
		memcpy(sig, &block[offsets[offsetIndex+1]], size);
		sig[size] = '\0';
		cout << "Offset #" << offsetIndex + 1 << ": size=" << size << " offset=" 
			<< offsets[offsetIndex+1] << endl;

// Use these lines instead for non-ascii data.
// 		cout << "   Signature: 0x";
// 		for (i = 0; i < size; i++)
// 			cout << hex << (((int) sig[i]) & 0xff);
		cout << "   Signature: " << sig;
		cout << dec << endl;
	}
}

int main(int argc, char **argv)
{
	int fd, err;
	char buf[BLOCK_SIZE];

	if (argc != 2)
		usage(argv[0]);

	fd = open(argv[1], O_RDONLY);
	if (fd < 0) {
		perror("open");
		exit(1);
	}
	err = read(fd, buf, BLOCK_SIZE);
	if (err < 0) {
		perror("read");
		exit(1);
	}
	if (err != BLOCK_SIZE) {
		cerr << "Failed to read the file in one op, check the filelength and try again." 
			<< endl;
		exit(1);
	}
	
	parseDictBlock(buf);
	exit(0);
}

