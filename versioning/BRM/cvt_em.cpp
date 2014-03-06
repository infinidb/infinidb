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
 * $Id: cvt_em.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 ****************************************************************************/

/*
 * Loads state of the BRM data structures from file.
 *
 * More detailed description
 */

#include <iostream>
#include <map>
using namespace std;

#include "extentmap.h"
using namespace BRM;

namespace
{
void usage(char *name)
{
	cout << "Usage: " << name << " <prefix>" << endl;
	exit(1);
}
}

namespace BRM
{
class ExtentMapConverter
{
public:
	ExtentMapConverter() {};

	int doCvt(unsigned oldExtentSize, unsigned newExtentSize, const string& filename);

private:
	ExtentMapConverter(const ExtentMapConverter& rhs);
	ExtentMapConverter& operator=(const ExtentMapConverter& rhs);

	ExtentMap em;
};

int ExtentMapConverter::doCvt(unsigned oldExtentSize, unsigned newExtentSize, const string&filename)
{
	int currentSize, loadSize[3];
	ifstream in;

	em.grabEMEntryTable(ExtentMap::WRITE);
	try {
		em.grabFreeList(ExtentMap::WRITE);
	}
	catch(...) {
		em.releaseEMEntryTable(ExtentMap::WRITE);
		throw;
	}

	div_t d = div((int)oldExtentSize, (int)newExtentSize);
	idbassert(d.quot > 1);
	idbassert(d.rem == 0);

	const unsigned mult = d.quot;

	in.open(filename.c_str());
	if (!in) {
		log_errno("ExtentMap::load(): open");
		em.releaseFreeList(ExtentMap::WRITE);
		em.releaseEMEntryTable(ExtentMap::WRITE);
		throw std::ios_base::failure("ExtentMap::load(): open failed. Check the error log.");
	}
	
	in.exceptions(ios_base::badbit | ios_base::failbit);
	
	try {
		in.read((char *) &loadSize, 3*sizeof(int));
	} 
	catch(...) {
		in.close();
		em.releaseFreeList(ExtentMap::WRITE);
		em.releaseEMEntryTable(ExtentMap::WRITE);
		throw;
	}
	
	const int emVersion = loadSize[0];
	const int emNumElements = loadSize[1];
	const int flNumElements = loadSize[2];
	/* What's a safe upper limit on the # of EM and FL entries? */
#define EM_MAGIC_V3 0x76f78b1e
	if ( emVersion != EM_MAGIC_V3 || emNumElements < 0 || flNumElements < 0) {
		in.close();
		em.releaseFreeList(ExtentMap::WRITE);
		em.releaseEMEntryTable(ExtentMap::WRITE);
		log("ExtentMap::load64(): That file is not a valid 64-bit ExtentMap image");
		throw std::runtime_error("ExtentMap::load64(): That file is not a valid 64-bit ExtentMap image");
	}

	memset(em.fExtentMap, 0, em.fEMShminfo->allocdSize);
	memset(em.fFreeList, 0, em.fFLShminfo->allocdSize);
	em.fEMShminfo->currentSize = 0;
	em.fFLShminfo->currentSize = 0;

	int j = 0;
	int maxLoops = (emNumElements * (signed)mult - em.fEMShminfo->allocdSize/sizeof(EMEntry)) / 100 + 1;
	int target = (int)(maxLoops * .02);
	if (maxLoops < 50) target = 1;
	// allocate shared memory for extent data	
	for (currentSize = em.fEMShminfo->allocdSize/sizeof(EMEntry);
			currentSize < (emNumElements * (signed)mult); 
			currentSize = em.fEMShminfo->allocdSize/sizeof(EMEntry)) {
		em.growEMShmseg();
		if ((j % target) == 0) cout << '.' << flush;
		j++;
	}
	cout << endl;

	// allocate shared memory for freelist
	for (currentSize = em.fFLShminfo->allocdSize/sizeof(InlineLBIDRange);
			currentSize < flNumElements;
			currentSize = em.fFLShminfo->allocdSize/sizeof(InlineLBIDRange)) {
		em.growFLShmseg();
	}
	
	try {
		typedef map<int, vector<int> > OIDMap_t;
		OIDMap_t OIDMap;
		uint8_t buf[emNumElements * sizeof(EMEntry)]; 
		uint8_t buf2[flNumElements * sizeof(InlineLBIDRange)];

		in.read((char *) buf, emNumElements * sizeof(EMEntry));

		//memcpy(fExtentMap, buf, emNumElements * sizeof(EMEntry));
		EMEntry* emSrc = reinterpret_cast<EMEntry*>(&buf[0]);
		j = 0;
		for (int i = 0; i < emNumElements; i++)
		{
			vector<int>& oidv = OIDMap[emSrc[i].fileID];
			for (unsigned k = 0; k < mult; k++)
			{
				oidv.push_back(j);
				//em.fExtentMap[j].range.start = emSrc[i].range.start;
				em.fExtentMap[j].range.start = emSrc[i].range.start + (k * newExtentSize);
				//em.fExtentMap[j].range.size = emSrc[i].range.size;
				em.fExtentMap[j].range.size = newExtentSize / 1024;
				em.fExtentMap[j].fileID = emSrc[i].fileID;
				em.fExtentMap[j].blockOffset = emSrc[i].blockOffset + (k * newExtentSize);
				em.fExtentMap[j].HWM = emSrc[i].HWM;
				em.fExtentMap[j].txnID = emSrc[i].txnID;
				em.fExtentMap[j].secondHWM = emSrc[i].secondHWM;
				em.fExtentMap[j].nextHeader = emSrc[i].nextHeader;
				em.fExtentMap[j].partition.type = emSrc[i].partition.type;
				//em.fExtentMap[j].partition.cprange.hi_val = emSrc[i].partition.cprange.hi_val;
				em.fExtentMap[j].partition.cprange.hi_val = numeric_limits<int64_t>::min();
				//em.fExtentMap[j].partition.cprange.lo_val = emSrc[i].partition.cprange.lo_val;
				em.fExtentMap[j].partition.cprange.lo_val = numeric_limits<int64_t>::max();
				//em.fExtentMap[j].partition.cprange.sequenceNum = emSrc[i].partition.cprange.sequenceNum;
				em.fExtentMap[j].partition.cprange.sequenceNum = 0;
				//em.fExtentMap[j].partition.cprange.isValid = emSrc[i].partition.cprange.isValid;
				em.fExtentMap[j].partition.cprange.isValid = CP_INVALID;
				j++;
			}
		}

		em.fEMShminfo->currentSize = j * sizeof(EMEntry);

		cout << j << " total new em entries from " << emNumElements << " file entries" << endl;
		cout << OIDMap.size() << " OIDs added to em" << endl;

		OIDMap_t::const_iterator iter = OIDMap.begin();
		OIDMap_t::const_iterator end = OIDMap.end();

		int l = 0;
		while (iter != end)
		{
			const vector<int>& oidv = iter->second;
			vector<int>::const_reverse_iterator riter = oidv.rbegin();
			vector<int>::const_reverse_iterator rend = oidv.rend();
			HWM_t hwm = em.fExtentMap[*riter].HWM;
			while (riter != rend)
			{
				if (em.fExtentMap[*riter].blockOffset > hwm)
				{
					em.fExtentMap[*riter].fileID = numeric_limits<int>::max();
					em.fExtentMap[*riter].blockOffset = 0;
					em.fExtentMap[*riter].HWM = 0;
					l++;
				}
				else break;
				++riter;
			}
			++iter;
		}

		cout << l << " entries moved to OID " << numeric_limits<int>::max() << endl;

#if 0
int k = j;
for (int j = 0; j < k; j++)
cout << em.fExtentMap[j].range.start << '\t' << em.fExtentMap[j].range.size << '\t' <<
em.fExtentMap[j].fileID << '\t' << em.fExtentMap[j].blockOffset << '\t' << em.fExtentMap[j].HWM << '\t' <<
em.fExtentMap[j].txnID << '\t' << em.fExtentMap[j].secondHWM << '\t' << em.fExtentMap[j].nextHeader << endl;
#endif
		in.read((char *) buf2, flNumElements * sizeof(InlineLBIDRange));

		//memcpy(fFreeList, buf2, flNumElements * sizeof(InlineLBIDRange));
		InlineLBIDRange* lrSrc = reinterpret_cast<InlineLBIDRange*>(&buf2[0]);
		j = 0;
		for (int i = 0; i < flNumElements; i++)
		{
			em.fFreeList[j].start = lrSrc[i].start;
			em.fFreeList[j].size = lrSrc[i].size;
			j++;
		}

		em.fFLShminfo->currentSize = j * sizeof(InlineLBIDRange);

		cout << j << " total new fl entries from " << flNumElements << " file entries" << endl;

	} catch(...) {
		in.close();
		em.releaseFreeList(ExtentMap::WRITE);
		em.releaseEMEntryTable(ExtentMap::WRITE);
		throw;
	}
	
	in.close();
	em.releaseFreeList(ExtentMap::WRITE);
	em.releaseEMEntryTable(ExtentMap::WRITE);

	return 0;
}

}

int main(int argc, char **argv)
{
	ExtentMapConverter emc;

	if (emc.doCvt(8192, 1024, "BRM_saves_em") != 0)
	{
		cerr << "Conversion failed." << endl;
		return 1;
	}
	else
	{
		cout << "OK" << endl;
		return 0;
	}
}
