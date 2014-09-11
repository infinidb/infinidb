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
* $Id: editem.cpp 1397 2011-02-03 17:56:12Z rdempsey $
*/

#include <iostream>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <sstream>
#include <string>
#include <unistd.h>
using namespace std;

#include "blocksize.h"
#include "calpontsystemcatalog.h"
#include "objectidmanager.h"
using namespace execplan;

#include "dbrm.h"
using namespace BRM;

#include "configcpp.h"
using namespace config;

#include "dataconvert.h"
using namespace dataconvert;

#include "liboamcpp.h"

#undef REALLY_DANGEROUS

#define CHECK(cmd) { int rc = (cmd);\
if ((rc) != 0)\
{ cerr << "Error in DBRM call " #cmd "; returnCode: " << rc << endl;\
return 1; } }

namespace {

OID_t MaxOID;

DBRM em;

string pname;

bool tflg = false;
bool sflg = false;
bool aflg = false;
bool fflg = false;
bool vflg = false;

void usage(const string& pname)
{
	cout << "usage: " << pname <<
	" [-tsahv] [-di]|[-o oid]|[-c oid]|[-x oid]|[-e oid]|[-r oid]|"
	"[-w oid]|[-l]|[-b lbid][-C]" << endl <<
		"   examins/modifies the extent map." << endl <<
		"   -o oid \tdisplay extent map for oid" << endl <<
		"   -d     \tdump the entire extent map" << endl <<
		"   -c oid \tclear the min/max vals for oid" << endl <<
		"   -C     \tclear all min/max vals" << endl <<
		"   -t     \tdisplay min/max values as dates" << endl <<
		"   -s     \tdisplay min/max values as timestamps" << endl <<
		"   -a     \tdisplay min/max values as char strings" << endl <<
		"   -x oid \tcreate/extend oid" << endl <<
		"   -e oid \tdelete oid" << endl <<
		"   -r oid \trollback or delete extents" << endl <<
		"   -v     \tdisplay verbose output" << endl <<
		"   -h     \tdisplay this help text" << endl <<
		"   -w oid \tedit HWM for an oid" << endl <<
		"   -l     \tdump the free list" << endl <<
		"   -b lbid\tdisplay info about lbid" << endl <<
		"   -i     \tformat the output for import (implies -d)" << endl;
}

const string charcolToString(int64_t v)
{
	ostringstream oss;
	char c;
	for (int i = 0; i < 8; i++)
	{
		c = v & 0xff;
		oss << c;
		v >>= 8;
	}
	return oss.str();
}

const string fmt(int64_t v)
{
	ostringstream oss;

	if (tflg)
	{
		oss << DataConvert::dateToString(v);
	}
	else if (sflg)
	{
		oss << DataConvert::datetimeToString(v);
	}
	else if (aflg)
	{
		oss << charcolToString(v);
	}
	else
	{
		oss << v;
	}

	return oss.str();
}

int dumpone(OID_t oid)
{
	std::vector<struct EMEntry> entries;
	std::vector<struct EMEntry>::iterator iter;
	std::vector<struct EMEntry>::iterator end;
	int64_t max;
	int64_t min;
	int32_t seqNum;
	bool header;
	bool needtrailer = false;
	unsigned extentRows = em.getExtentRows();
	unsigned colWidth = 0;

	CHECK(em.getExtents(oid, entries, false, false, true));
	if (entries.size() > 0)
	{
		header = false;
		iter = entries.begin();
		end = entries.end();
		while (iter != end)
		{
			max=-1;
			min=-1;
			seqNum=0;
			u_int32_t lbidRangeSize = iter->range.size * 1024;
			int state = em.getExtentMaxMin(iter->range.start, max, min, seqNum);
			if (!header)
			{
				if ( iter->colWid > 0 )
				{
					HWM_t hwm;
					CHECK(em.getHWM(oid, hwm));
					cout << "Col OID = " << oid << ", HWM = " << hwm <<
						", width = " << iter->colWid;
					uint32_t partNum;
					uint16_t segNum;
					uint16_t dbRoot;
					CHECK(em.getLastLocalHWM(oid,dbRoot,partNum,segNum,hwm));
					cout << ";   LastLocalHWM: DBRoot-" << dbRoot <<
						", part#-" << partNum << ", seg#-" << segNum <<
						", HWM-" << hwm << endl;
					colWidth = iter->colWid;
				}
				else
				{
					cout << "Dct OID = " << oid << endl;
					colWidth = DICT_COL_WIDTH;
				}
				header = true;
			}

			if (vflg)
				cout << oid << ' ';
			cout << iter->range.start << " - " <<
				(iter->range.start + lbidRangeSize - 1) <<
				" (" << lbidRangeSize << ") min: " << fmt(min) <<
				", max: " << fmt(max) << ", seqNum: " << seqNum << ", state: ";

			switch (state)
			{
			case 0:
				cout << "invalid";
				break;
			case 1:
				cout << "updating";
				break;
			case 2:
				cout << "valid";
				break;
			default:
				cout << "unknown";
				break;
			}
			cout << ", fbo: "   << iter->blockOffset;
			cout << ", DBRoot: " << iter->dbRoot <<
				", part#: " << iter->partitionNum <<
				", seg#: " << iter->segmentNum;
			cout << ", HWM: "   << iter->HWM;
			switch (iter->status)
			{
			case EXTENTAVAILABLE:
				cout << "; status: avail";
				break;
			case EXTENTUNAVAILABLE:
				cout << "; status: unavail";
				break;
			case EXTENTOUTOFSERVICE:
				cout << "; status: outOfSrv";
				break;
			default:
				cout << "; status: unknown";
				break;
			}
			cout << endl;

			//Complain loudly if there's a mis-match
			if (lbidRangeSize != (extentRows*colWidth / BLOCK_SIZE))
			{
				cout << endl;
				throw logic_error(
					"Extent Map entries do match config file setting!");
			}

			needtrailer = true;
			++iter;
		}
		if (needtrailer) cout << endl;
	}
	return 0;
}

int dumpall()
{
	for (OID_t oid = 0; oid <= MaxOID; oid++)
	{
		dumpone(oid);
	}

	return 0;
}

int zapit()
{
#ifdef REALLY_DANGEROUS
	LBIDRange_v range;

	for (OID_t oid = 0; oid <= MaxOID; oid++)
	{
		CHECK(em.lookup(oid, range));
		if (range.size() > 0)
		{
			CHECK(em.deleteOID(oid));
			CHECK(em.confirmChanges());
		}
	}
#else
	cerr << "Sorry, I'm not going to do that." << endl;
#endif
	return 0;
}

int clearAllCPData()
{
	BRM::LBIDRange_v ranges;
	int oid, err;

	for (oid = 0; oid < MaxOID; oid++) {
		err = em.lookup(oid, ranges);
		if (err == 0 && ranges.size() > 0) {
			BRM::CPInfo cpInfo;
			BRM::CPInfoList_t vCpInfo;
			cpInfo.max = numeric_limits<int64_t>::min();
			cpInfo.min = numeric_limits<int64_t>::max();
			cpInfo.seqNum = -1;

			for (uint i=0; i< ranges.size(); i++) {
				BRM::LBIDRange r=ranges.at(i); 
				cpInfo.firstLbid = r.start;
				vCpInfo.push_back(cpInfo);
			}
			CHECK(em.setExtentsMaxMin(vCpInfo));
		}
	}
	return 0;
}

int clearmm(OID_t oid)
{

	BRM::LBIDRange_v ranges;
	CHECK(em.lookup(oid, ranges));
	BRM::LBIDRange_v::size_type rcount = ranges.size();

	// @bug 2280.  Changed to use the batch interface to clear the CP info to make the clear option faster.
	BRM::CPInfo cpInfo;
	BRM::CPInfoList_t vCpInfo;
	cpInfo.max = numeric_limits<int64_t>::min();
	cpInfo.min = numeric_limits<int64_t>::max();
	cpInfo.seqNum = -1;

	for (unsigned i = 0; i < rcount; i++) {
		BRM::LBIDRange r = ranges.at(i); 
		cpInfo.firstLbid = r.start;
		vCpInfo.push_back(cpInfo);
	}
	CHECK(em.setExtentsMaxMin(vCpInfo));

	return 0;
}

int extendOid(OID_t oid)
{
	uint32_t  partNum;
	uint16_t  segNum;
	uint16_t  dbRoot;
	char      DictStoreOIDFlag;
	u_int32_t colWidth;
	int       size;
	LBID_t    lbid;
	u_int32_t startBlock;
	int       allocd;

	unsigned extentRows = em.getExtentRows();
	cout << "Enter number of extents to allocate to " << oid <<
		" (extent size is " << extentRows << " rows): ";

	string resp;
	cin >> resp;
	if (resp.empty())
		return 1;
	size = (int)strtol(resp.c_str(), 0, 0);
	if (size <= 0)
	{
		cerr << "size must be greater than 0." << endl;
		usage(pname);
		return 1;
	}

	cout << "Are you extending a dictionary store oid (y/n)? ";
	cin >> DictStoreOIDFlag;

	if ((DictStoreOIDFlag == 'y') || (DictStoreOIDFlag == 'Y'))
	{
		cout << "Enter DBRoot, and Partition#, and Segment# "
			"(separated by spaces): ";
		cin >> dbRoot >> partNum >> segNum;

		for (int i=0; i<size; i++)
		{
			CHECK(em.createDictStoreExtent ( oid, dbRoot, partNum, segNum,
				lbid, allocd));

			if (vflg)
			{
				cout << oid << " created/extended w/ " << allocd << " blocks; "
				"beginning LBID: " << lbid << 
				"; DBRoot: " << dbRoot <<
				"; Part#: "  << partNum <<
				"; Seg#: "   << segNum << endl;
			}
		}
	}
	else
	{
		cout << "Enter column width, DBRoot, and part# (separated by spaces), " << 
			endl << "DBRoot and part# will only be used if it's the first extent: ";
		cin >> colWidth >> dbRoot >> partNum;

		for (int i=0; i<size; i++)
		{
			CHECK(em.createColumnExtent ( oid, colWidth, dbRoot, partNum,
				segNum, lbid, allocd, startBlock));

			if (vflg)
			{
				cout << oid << " created/extended w/ " << allocd << " blocks; "
				"beginning LBID: " << lbid << 
				"; startBlkOffset: " << startBlock << 
				"; DBRoot: " << dbRoot <<
				"; Part#: "  << partNum <<
				"; Seg#: "   << segNum << endl;
			}
		}
	}

	return 0;
}

int rollbackExtents(OID_t oid)
{
	char      DictStoreOIDFlag;
	uint32_t  partNum;
	HWM_t     hwm;

	cout << "Are you rolling back extents for a dictionary store oid (y/n)? ";
	cin >> DictStoreOIDFlag;

	if ((DictStoreOIDFlag == 'y') || (DictStoreOIDFlag == 'Y'))
	{
		unsigned int hwmCount;
		vector<HWM_t> hwms;

		cout << "Enter part#, and the number of HWMs to be entered (separated by spaces): ";
		cin >> partNum >> hwmCount;
		for (unsigned int k=0; k<hwmCount; k++)
		{
			cout << "Enter HWM for the last extent in segment file " << k << ": ";
			cin >> hwm;
			hwms.push_back(hwm);
		}

		CHECK(em.rollbackDictStoreExtents( oid, partNum, hwms ));
	}
	else
	{
		uint16_t  segNum;

		cout << "Enter part#, seg#, and HWM for the last extent (separated by spaces): ";
		cin >> partNum >> segNum >> hwm;

		CHECK(em.rollbackColumnExtents( oid, partNum, segNum, hwm ));
	}

	return 0;
}

int deleteOid(OID_t oid)
{
	if (!fflg)
	{
		cout << "WARNING! This operation cannot be undone. Enter 'yes' to continue: ";
		string resp;
		cin >> resp;
		if (resp.empty())
			return 1;
		string::const_iterator p = resp.begin();
		if (*p != 'y' && *p != 'Y')
			return 1;
	}

	cout << "Deleting extent map info for " << oid << endl;

	CHECK(em.deleteOID(oid));

	return 0;
}

int editHWM(OID_t oid)
{
	HWM_t    oldHWM;
	HWM_t    newHWM;
	uint32_t partNum   = 0;
	uint16_t segNum    = 0;

	cout << "Enter Partition#, and Segment# (separated by spaces): ";
	cin >> partNum >> segNum;
	CHECK(em.getLocalHWM(oid, partNum, segNum, oldHWM));

	cout << "HWM for partition " << partNum << " and segment " << segNum <<
		" is currently " << oldHWM <<
		".  Enter new value: ";
	cin >> newHWM;

	CHECK(em.setLocalHWM(oid, partNum, segNum, newHWM));

	return 0;
}

int dumpFL()
{
	vector<InlineLBIDRange> v = em.getEMFreeListEntries();

	vector<InlineLBIDRange>::iterator iter = v.begin();
	vector<InlineLBIDRange>::iterator end = v.end();

	while (iter != end)
	{
		if (iter->size || vflg)
			cout << iter->start << '\t' << iter->size << endl;
		++iter;
	}

	return 0;
}

int dumpLBID(LBID_t lbid)
{
	uint16_t ver = 0;
	BRM::OID_t oid;
	uint16_t dbroot;
	uint32_t partNum;
	uint16_t segNum;
	uint32_t fbo;
	int rc;
	rc = em.lookupLocal(lbid, ver, false, oid, dbroot, partNum, segNum, fbo);
	assert(rc == 0);
	cout << "LBID " << lbid << " is part of OID " << oid <<
		"; DbRoot "     << dbroot  <<
		"; partition# " << partNum <<
		"; segment# "   << segNum  <<
		"; at FBO "     << fbo     << endl;
	return 0;
}

}

int main(int argc, char** argv)
{
	int c;
	pname = argv[0];
	bool dflg = false;
	int zflg = 0;
	bool cflg = false;
	bool Cflg = false;
	OID_t oid = 0;
	bool oflg = false;
	bool xflg = false;
	bool eflg = false;
	bool rflg = false;
	bool wflg = false;
	bool lflg = false;
	bool bflg = false;
	bool iflg = false;
	LBID_t lbid = 0;

	opterr = 0;

	while ((c = getopt(argc, argv, "dzCc:o:tsx:e:r:fvhw:lb:ai")) != EOF)
		switch (c)
		{
		case 'd':
			dflg = true;
			break;
		case 'z':
			zflg++;
			break;
		case 'C':
			Cflg = true;
			break;
		case 'c':
			cflg = true;
			oid = (OID_t)strtoul(optarg, 0, 0);
			break;
		case 'o':
			oflg = true;
			oid = (OID_t)strtoul(optarg, 0, 0);
			break;
		case 't':
			tflg = true;
			break;
		case 's':
			sflg = true;
			break;
		case 'x':
			xflg = true;
			oid = (OID_t)strtoul(optarg, 0, 0);
			break;
		case 'e':
			eflg = true;
			oid = (OID_t)strtoul(optarg, 0, 0);
			break;
		case 'r':
			rflg = true;
			oid = (OID_t)strtoul(optarg, 0, 0);
			break;
		case 'f':
			fflg = true;
			break;
		case 'v':
			vflg = true;
			break;
		case 'w':
			wflg = true;
			oid = (OID_t)strtoul(optarg, 0, 0);
			break;
		case 'l':
			lflg = true;
			break;
		case 'b':
			bflg = true;
			lbid = (LBID_t)strtoull(optarg, 0, 0);
			break;
		case 'a':
			aflg = true;
			break;
		case 'i':
			iflg = true;
			break;
		case 'h':
		case '?':
		default:
			usage(pname);
			return (c == 'h' ? 0 : 1);
			break;
		}

	(void)Config::makeConfig();

	//IF this is UM in a multi-node system, there may not (won't) be an OID bitmap file, so move on...
	oam::oamModuleInfo_t modInfo;
	oam::Oam oam;
	string localModuleType("pm");
	try
	{
		modInfo = oam.getModuleInfo();
		localModuleType = modInfo.get<1>();
	}
	catch (...)
	{
	}

	MaxOID = -1;
	if (localModuleType != "um")
	{
		ObjectIDManager oidm;
		MaxOID = oidm.size();
	}

	if (em.isReadWrite() != ERR_OK)
	{
		cerr << endl << "Warning! The DBRM is currently in Read-Only mode!" << endl
			<< "Updates will not propagate!" << endl << endl;
	}

	if (iflg)
	{
		dflg = true;
		ExtentMap em;
		cout << em;
		return 0;
	}

	if ((int)dflg + (int)cflg + (int)oflg + (int)xflg + (int)eflg + (int)wflg +
		(int)lflg + (int)bflg + (int)Cflg + (int)rflg > 1)
	{
		cerr << "Only one of d/c/o/x/e/w/l/b/r/C can be specified." << endl;
		usage(pname);
		return 1;
	}

	if ((int)sflg + (int)tflg + (int)aflg > 1)
	{
		cerr << "Only one of s/t/a can be specified." << endl;
		usage(pname);
		return 1;
	}

	if (MaxOID < 0 && ((int)dflg + (int)Cflg + zflg) > 0)
	{
		cerr << "Can't use d/C flag on module type " << localModuleType << endl;
		usage(pname);
		return 1;
	}

	if (oflg)
		return dumpone(oid);

	if (dflg)
		return dumpall();

	if (zflg >= 2)
		return zapit();
	else if (zflg)
	{
		cerr << "Not enough z's to zap extent map." << endl;
		return 1;
	}
	if (Cflg)
		return clearAllCPData();
	if (cflg)
		return clearmm(oid);
	if (xflg)
		return extendOid(oid);
	if (eflg)
		return deleteOid(oid);
	if (wflg)
		return editHWM(oid);
	if (rflg)
		return rollbackExtents(oid);
	if (lflg)
		return dumpFL();
	if (bflg)
		return dumpLBID(lbid);

	usage(pname);

	return 0;
}
// vim:ts=4 sw=4:

