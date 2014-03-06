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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "we_index.h"
#include "calpontsystemcatalog.h"

using namespace std;

void usage()
{
	cout << "Usage: print_indextree_subblock filename block_offset subblock_number" << endl;
	exit(1);
}

int main(int argc, char **argv) 
{
	char buf[256];
	int fd, err, subblock, fbo, byteoffset, i;
	string filename;
	WriteEngine::IdxBitTestEntry *entry;

	if (argc != 4)
		usage();
	
	filename = argv[1];
	fbo = atoi(argv[2]);
	subblock = atoi(argv[3]);

	cout << "FBO: " << fbo << " Subblock: " << subblock << endl;
	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		exit(1);
	}

	byteoffset = fbo * BLOCK_SIZE + subblock * 256;
	lseek(fd, byteoffset, SEEK_SET);
	err = read(fd, buf, 256);
	if (err != 256) {
		perror("read");
		exit(1);
	}
	close(fd);

	for (i = 0, byteoffset = 0; byteoffset < 256; 
	  byteoffset += sizeof(WriteEngine::IdxBitTestEntry), i++) {
		entry = (WriteEngine::IdxBitTestEntry *) &buf[byteoffset];
		cout << "Entry " << i << ": fbo=" << (int)entry->fbo << 
			" sbid=" << entry->sbid << " sbentry=" << entry->entry <<
			" group=" << entry->group << " bittest=" << entry->bitTest <<
			" type=" << entry->type << endl;
	}

	exit(0);
}
