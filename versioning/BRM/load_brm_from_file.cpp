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

/*
 * A tool that can take a csv file of extent map entires and produce a BRM_saves_em-compatible file.
 * If you re-compile extentmap.cpp to dump the extent map as it loads, you'll get a csv file on stdout.
 * Save this to a file and edit it as needed (remove the cruft at the top & bottom for sure). Then use
 * this tool to create a binary BRM_saves_em file.
 *
 * compile with
 * g++ -g -Wall -o load_brm_from_file -I$HOME/genii/export/include -I/usr/include/libxml2 load_brm_from_file.cpp
 *
*/
#include <iostream>
#include <stdint.h>
#include <fstream>
#include <cerrno>
#include <string>
#include <cstdlib>
//#define NDEBUG
#include <cassert>
#include <limits>
using namespace std;

#include <boost/tokenizer.hpp>
using namespace boost;

#include "extentmap.h"
using namespace BRM;

#define EM_MAGIC_V4 0x76f78b1f

namespace BRM
{
EMEntry::EMEntry()
{
	fileID = 0;
	blockOffset = 0;
	HWM = 0;
	partitionNum = 0;
	segmentNum   = 0;
	dbRoot       = 0;
	colWid       = 0;
	status		= 0;
}
EMCasualPartition_struct::EMCasualPartition_struct()
{
	lo_val=numeric_limits<int64_t>::min();
	hi_val=numeric_limits<int64_t>::max();
	sequenceNum=0;
	isValid = CP_INVALID;
}
}

int main(int argc, char** argv)
{
	int e;

	int loadSize[3];

	if (argc < 2)
	{
		cerr << "filename arg needed" << endl;
		return 1;
	}

	ifstream in(argv[1]);
	e = errno;

	if (!in)
	{
		cerr << "file read error: " << strerror(e) << endl;
		return 1;
	}

	//Brute force count the number of lines
	int numEMEntries = 0;

	string line;

	getline(in, line);

	while (!in.eof())
	{
		numEMEntries++;
		getline(in, line);
	}

	//start at the beginning again...
	in.clear();
	in.seekg(0, ios_base::beg);

	idbassert(in.good());
	idbassert(in.tellg() == static_cast<streampos>(0));

	string outname(argv[1]);
	outname += ".out";

	ofstream out(outname.c_str());
	e = errno;

	if (!out)
	{
		cerr << "file write error: " << strerror(e) << endl;
		return 1;
	}

	loadSize[0] = EM_MAGIC_V4;
	loadSize[1] = numEMEntries;
	loadSize[2] = 1; //one free list entry
	out.write((char *)&loadSize, (3 * sizeof(int)));

	InlineLBIDRange fl;
	fl.start = 0;
	//the max lbid is 2^54-1, the size is in units of 1k
	fl.size = numeric_limits<uint32_t>::max();

	InlineLBIDRange maxLBIDinUse;
	maxLBIDinUse.start = 0;
	maxLBIDinUse.size = 0;

	getline(in, line);

	while (!in.eof())
	{
		EMEntry em;
		int64_t v;
		tokenizer<> tok(line);
		tokenizer<>::iterator beg=tok.begin();
#if 0
			emSrc[i].range.start
			<< '\t' << emSrc[i].range.size
			<< '\t' << emSrc[i].fileID
			<< '\t' << emSrc[i].blockOffset
			<< '\t' << emSrc[i].HWM
			<< '\t' << emSrc[i].partitionNum
			<< '\t' << emSrc[i].segmentNum
			<< '\t' << emSrc[i].dbRoot
			<< '\t' << emSrc[i].colWid
			<< '\t' << emSrc[i].status
			<< '\t' << emSrc[i].partition.cprange.hi_val
			<< '\t' << emSrc[i].partition.cprange.lo_val
			<< '\t' << emSrc[i].partition.cprange.sequenceNum
			<< '\t' << (int)(emSrc[i].partition.cprange.isValid)
#endif
		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.range.start = v;

		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.range.size = v;

		if (em.range.start > maxLBIDinUse.start)
		{
			maxLBIDinUse.start = em.range.start;
			maxLBIDinUse.size = em.range.size;
		}

		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.fileID = v;

		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.blockOffset = v;

		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.HWM = v;

		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.partitionNum = v;

		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.segmentNum = v;

		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.dbRoot = v;

		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.colWid = v;

		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.status = v;

		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.partition.cprange.hi_val = v;

		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.partition.cprange.lo_val = v;

		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.partition.cprange.sequenceNum = v;

		v = strtoll(beg->c_str(), 0, 0);
		++beg;
		em.partition.cprange.isValid = v;

		out.write((char *)&em, sizeof(em));

		getline(in, line);
	}

	fl.start = maxLBIDinUse.start + maxLBIDinUse.size * 1024;
	fl.size -= fl.start / 1024;

	out.write((char *)&fl, sizeof(fl));

	out.close();
	in.close();

	return 0;
}

