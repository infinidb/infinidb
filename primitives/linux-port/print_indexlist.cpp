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

#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>

#include "PrimitiveMsg.h"
#include "calpontsystemcatalog.h"
#include "we_index.h"


/** @file
 * Brief description of the file contents
 *
 * More detailed description
 */

using namespace std;

void usage()
{
	cout << "Usage: print_indexlist filename <S> block_offset subblock sbentry" << endl;
	cout << "   Where S=0 means print the whole block, S=1 means print the subblock" << endl;
	cout << "   Where subblock_number indicates which subblock to print." << endl;
	cout << "   Where sbentry indicates the first entry to print" << endl;
	exit(1);
}

int main(int argc, char **argv) 
{
	char buf[8192];
	int s, fd, subblock, byteoffset, i, entries, sbentry;
	string filename;
	off_t offset;
	off_t err;
	IndexListEntry *entry;
	IndexListParam *ptr;
	uint32_t fbo;

	if (argc == 1)
		usage();
	
	filename = argv[1];
	s = atoi(argv[2]);
	fbo = strtoul(argv[3], 0, 0);
	subblock = atoi(argv[4]);
	sbentry = atoi(argv[5]);

	fd = open(filename.c_str(), O_RDONLY);
	if (fd < 0) {
		perror("open");
		exit(1);
	}
	offset = ((off_t)fbo * BLOCK_SIZE);
// 	cout << "BLOCK_SIZE = " << BLOCK_SIZE << " fbo=" << fbo << ", seeking to offset " << offset << endl;
	err = lseek(fd, offset, SEEK_SET);
	if (err < 0) {
		perror("lseek");
		exit(1);
	}
	err = read(fd, buf, BLOCK_SIZE);
	if (err != BLOCK_SIZE) {
		if (err < 0)
			perror("read");
		cerr << "read error." << endl;
		exit(1);
	}
	close(fd);
	
	byteoffset = subblock * 256;
// 	cout << "subblock=" << subblock << " byte offset=" << byteoffset << endl;
	entry = reinterpret_cast<IndexListEntry *>(&buf[byteoffset]);
	if (s == 0 && subblock == 0)
		entries = 1024;
	else
		entries = 32;
	
	for (i = sbentry; i < entries; i++) {
		
		cout << i << ": ";
		switch (entry[i].type) {
			case LLP_SUBBLK:
				ptr = reinterpret_cast<IndexListParam *>(&entry[i]);
				cout << "Subblock pointer.  Rid count=" << entry[i].ridCt << 
					" LBID=" << ptr->fbo <<  " subblock=" << ptr->sbid << 
					" SBentry=" << ptr->entry << endl;
				break;
		    case LLP_BLK:
				ptr = reinterpret_cast<IndexListParam *>(&entry[i]);
				cout << "Block pointer.  Rid count=" << entry[i].ridCt << 
					" LBID=" << ptr->fbo <<  " subblock=" << 
					ptr->sbid << " SBentry=" << ptr->entry << endl;
				break;
			case RID:
				cout << "RID: " << entry[i].value << endl;
				break;
			case LIST_SIZE:
				if (i == sbentry) {
					u_int64_t *val = reinterpret_cast<u_int64_t *>(&entry[i+1]);
					cout << "List Header.  Rid count=" << entry[i].value;
					cout << " key value=0x" << hex << *val << dec << endl;
					i++;
				}
				else if (i + 1 < entries) {
					u_int64_t *val = reinterpret_cast<u_int64_t *>(&entry[i+1]);
					cout << "List Size entry.  Rid count=" << entry[i].value
					  << " (if a header) value=0x" << hex << *val << dec << endl;
				}
				else 
					cout << "List Size entry.  Rid count=" << entry[i].value << endl;
				break;
	    	case NOT_IN_USE:
				cout << "Not in use (ignored by p_idxlist)" << endl;
				break;
	    	case EMPTY_LIST_PTR:
				cout << "Empty List Pointer (?) (ignored by p_idxlist)" << endl;
				break;
	    	case EMPTY_PTR:
				cout << "Empty Pointer entry (?) (ignored by p_idxlist)" << endl;
				break;
			default:
				cout << "Unknown entry type (" << entry[i].type << ")" << endl;
				break;
		}
	}
	return 0;
}
