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
* $Id: editem.cpp 2336 2013-06-25 19:11:36Z rdempsey $
*/

#include <iostream>
#include <vector>
#include <algorithm>
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

DBRM* emp=0;

string pname;

bool tflg = false;
bool sflg = false;
bool aflg = false;
bool fflg = false;
bool vflg = false;
bool mflg = false;
bool uflg = false;

struct SortExtentsByPartitionFirst
{
	bool operator() (const EMEntry& entry1, const EMEntry& entry2)
	{
		if ( (entry1.partitionNum <  entry2.partitionNum) ||
			((entry1.partitionNum == entry2.partitionNum) &&
			 (entry1.dbRoot       <  entry2.dbRoot))      ||
			((entry1.partitionNum == entry2.partitionNum) &&
			 (entry1.dbRoot       == entry2.dbRoot)       &&
			 (entry1.segmentNum   <  entry2.segmentNum))  ||
			((entry1.partitionNum == entry2.partitionNum) &&
			 (entry1.dbRoot       == entry2.dbRoot)       &&
			 (entry1.segmentNum   == entry2.segmentNum)   &&
			 (entry1.blockOffset  <  entry2.blockOffset)) )
			return true;
		else
			return false;
	}
};

struct SortExtentsByDBRootFirst
{
	bool operator() (const EMEntry& entry1, const EMEntry& entry2)
	{
		if ( (entry1.dbRoot       <  entry2.dbRoot)       ||
			((entry1.dbRoot       == entry2.dbRoot)       &&
			 (entry1.partitionNum <  entry2.partitionNum))||
			((entry1.dbRoot       == entry2.dbRoot)       &&
			 (entry1.partitionNum == entry2.partitionNum) &&
			 (entry1.segmentNum   <  entry2.segmentNum))  ||
			((entry1.dbRoot       == entry2.dbRoot)       &&
			 (entry1.partitionNum == entry2.partitionNum) &&
			 (entry1.segmentNum   == entry2.segmentNum)   &&
			 (entry1.blockOffset  <  entry2.blockOffset)) )
			return true;
		else
			return false;
	}
};

//------------------------------------------------------------------------------
// Describes program usage to the user
//------------------------------------------------------------------------------
void usage(const string& pname)
{
	cout << "usage: " << pname <<
	" [-tsahvm] [-di]|[-o oid -S opt]|[-c oid]|[-x]|[-e oid]|[-r oid]|"
	"[-w oid]|[-l]|[-b lbid][-C][-p dbr]" << endl <<
		"   examins/modifies the extent map." << endl <<
		"   -h     \tdisplay this help text" << endl <<
		"   -o oid \tdisplay extent map for oid" << endl <<
		"   -S opt \tSort order for -o (1-partition, dbroot, seg#, fbo;"<<endl<<
		"          \t                   2-dbroot, partition, seg#, fbo;"<<endl<<
		"          \t                   default is unsorted)"  << endl <<
		"   -d     \tdump the entire extent map" << endl <<
		"   -c oid \tclear the min/max vals for oid" << endl <<
		"   -C     \tclear all min/max vals" << endl <<
		"   -t     \tdisplay min/max values as dates" << endl <<
		"   -s     \tdisplay min/max values as timestamps" << endl <<
		"   -a     \tdisplay min/max values as char strings" << endl <<
        "   -u     \tdisplay min/max values as unsigned integers" << endl <<
		"   -x     \tcreate/extend one or more oids" << endl <<
		"   -e oid \tdelete oid" << endl <<
		"   -r oid \trollback or delete extents" << endl <<
		"   -v     \tdisplay verbose output" << endl <<
		"   -w oid \tedit HWM for an oid" << endl <<
		"   -l     \tdump the free list" << endl <<
		"   -b lbid\tdisplay info about lbid" << endl <<
		"   -i     \tformat the output for import (implies -dm)" << endl <<
		"   -m     \tdisplay actual min/max values" << endl <<
		"   -p dbr \tdelete all extents on dbroot dbr" << endl;
}

//------------------------------------------------------------------------------
// Converts a non-dictionary char column to a string
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
// Formats an integer to it's date, datetime, or char equivalent
//------------------------------------------------------------------------------
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
	else if (mflg)
	{
		oss << v;
	}
    else if (uflg)
    {
        if (static_cast<uint64_t>(v) > numeric_limits<uint64_t>::max() - 2)
            oss << "notset";
        else
            oss << static_cast<uint64_t>(v);
    }
	else
	{
		if (v == numeric_limits<int64_t>::max() ||
			v <= (numeric_limits<int64_t>::min() + 2))
			oss << "notset";
		else
			oss << v;
	}

	return oss.str();
}

//------------------------------------------------------------------------------
// Check to see if the latest read operation from stdin was successful.
// Primarily used to validate the case where we have prompted the user for
// more than 1 parameter.  Ex: we are validating that the user correctly
// formatted the input to enter 3 values separated by spaces (1 2 3) in-
// stead of accidentally separating with commas (1,2,3).
//------------------------------------------------------------------------------
//@bug 4914: Validate that correct number of input parameters is given
bool isInputValid()
{
	if (cin.good())
		return true;

	cin.clear();
	cin.ignore( numeric_limits<std::streamsize>::max(),'\n');
	cout << endl << "Invalid input; try again, be sure to enter spaces "
		"between input parameters." << endl << endl;

	return false;
}

//------------------------------------------------------------------------------
// Dump all the extents for the specified OID
//------------------------------------------------------------------------------
int dumpone(OID_t oid, unsigned int sortOrder)
{
	std::vector<struct EMEntry> entries;
	std::vector<struct EMEntry>::iterator iter;
	std::vector<struct EMEntry>::iterator end;
	int64_t max;
	int64_t min;
	int32_t seqNum;
	bool header;
	bool needtrailer = false;
	unsigned extentRows = emp->getExtentRows();
	unsigned colWidth = 0;

	CHECK(emp->getExtents(oid, entries, false, false, true));
	if (entries.size() > 0)
	{
		if (sortOrder == 1) {
			SortExtentsByPartitionFirst sorter;
			std::sort( entries.begin(), entries.end(), sorter );
		}
		else if (sortOrder == 2) {
			SortExtentsByDBRootFirst sorter;
			std::sort( entries.begin(), entries.end(), sorter );
		}

		header = false;
		iter = entries.begin();
		end = entries.end();
		while (iter != end)
		{
			uint32_t lbidRangeSize = iter->range.size * 1024;
			max       = iter->partition.cprange.hi_val;
			min       = iter->partition.cprange.lo_val;
			seqNum    = iter->partition.cprange.sequenceNum;
			int state = iter->partition.cprange.isValid;
			if (!header)
			{
				if ( iter->colWid > 0 )
				{
					cout << "Col OID = " << oid << ", NumExtents = " <<
						entries.size() << ", width = " << iter->colWid << endl;
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

//------------------------------------------------------------------------------
// Dumps all the extents in the extent map
//------------------------------------------------------------------------------
int dumpall()
{
	for (OID_t oid = 0; oid <= MaxOID; oid++)
	{
		dumpone(oid, 0 /* no sorting */);
	}

	return 0;
}

//------------------------------------------------------------------------------
// Deletes all the extents in the extent map
//------------------------------------------------------------------------------
int zapit()
{
#ifdef REALLY_DANGEROUS
	LBIDRange_v range;

	for (OID_t oid = 0; oid <= MaxOID; oid++)
	{
		CHECK(emp->lookup(oid, range));
		if (range.size() > 0)
		{
			CHECK(emp->deleteOID(oid));
			CHECK(emp->confirmChanges());
		}
	}
#else
	cerr << "Sorry, I'm not going to do that." << endl;
#endif
	return 0;
}

//------------------------------------------------------------------------------
// Clears Casual Partition min/max for all the extents in the extent map
//------------------------------------------------------------------------------
int clearAllCPData()
{
	BRM::LBIDRange_v ranges;
	int oid, err;

	for (oid = 0; oid < MaxOID; oid++) {
		err = emp->lookup(oid, ranges);
		if (err == 0 && ranges.size() > 0) {
			BRM::CPInfo cpInfo;
			BRM::CPInfoList_t vCpInfo;
			cpInfo.max = numeric_limits<int64_t>::min();
			cpInfo.min = numeric_limits<int64_t>::max();
			cpInfo.seqNum = -1;

			for (uint32_t i=0; i< ranges.size(); i++) {
				BRM::LBIDRange r=ranges.at(i); 
				cpInfo.firstLbid = r.start;
				vCpInfo.push_back(cpInfo);
			}
			CHECK(emp->setExtentsMaxMin(vCpInfo));
		}
	}
	return 0;
}

//------------------------------------------------------------------------------
// Clears Casual Partition min/max for the specified OID
//------------------------------------------------------------------------------
int clearmm(OID_t oid)
{

	BRM::LBIDRange_v ranges;
	CHECK(emp->lookup(oid, ranges));
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
	CHECK(emp->setExtentsMaxMin(vCpInfo));

	return 0;
}

//------------------------------------------------------------------------------
// Create/add extents to dictionary OID, or a list of column OIDs.
//------------------------------------------------------------------------------
int extendOids( )
{
	uint16_t  dbRoot;
	uint32_t  partNum;
	uint16_t  segNum;
	OID_t     oid;
	uint32_t colWidth;
	char      DictStoreOIDFlag;

	vector<CreateStripeColumnExtentsArgIn> cols;

	cout << "Are you extending a dictionary store oid (y/n)? ";
	cin >> DictStoreOIDFlag;

	if ((DictStoreOIDFlag == 'y') || (DictStoreOIDFlag == 'Y'))
	{
		LBID_t lbid;
		int    allocd;

		cout << "Enter OID, DBRoot, and Partition#, and Segment# "
			"(separated by spaces): ";
		while (1)
		{
			cin >> oid >> dbRoot >> partNum >> segNum;
			if (isInputValid())
				break;
		}

		CHECK(emp->createDictStoreExtent ( oid, dbRoot, partNum, segNum,
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
	else
	{
		while (1)
		{
			bool bFinished = false;

			while (1)
			{
				cout << "Enter OID and column width (separated by spaces); "
					"0 OID represents end of list: ";
				cin >> oid >> colWidth;
				if (oid == 0)
				{
					bFinished = true;
					break;
				}

				if (isInputValid())
					break;
			}
			if (bFinished)
				break;

			CreateStripeColumnExtentsArgIn colArg;
			colArg.oid   = oid;
			colArg.width = colWidth;
			if (uflg)
			{
				colArg.colDataType = execplan::CalpontSystemCatalog::UBIGINT;
			}
			else
			{
				colArg.colDataType = execplan::CalpontSystemCatalog::BIGINT;
			}
			cols.push_back( colArg );
		}

		vector<CreateStripeColumnExtentsArgOut> newExtents;

		while (1)
		{
			cout << "Enter DBRoot and partition# (partition "
				"only used for empty DBRoot): ";
			cin  >> dbRoot >> partNum;
			if (isInputValid())
				break;
		}
	
		CHECK(emp->createStripeColumnExtents(cols, dbRoot, partNum,
			segNum, newExtents));

		cout << "Extents created in partition " << partNum <<
			", segment " << segNum << endl;
		if (vflg)
		{
			for (unsigned k=0; k<newExtents.size(); k++)
			{
				cout << "Column OID-" << cols[k].oid <<
					"; LBID-"  << newExtents[k].startLbid <<
					"; nblks-" << newExtents[k].allocSize <<
					"; fbo-"   << newExtents[k].startBlkOffset << endl;
			}
		}
	}

	return 0;
}

//------------------------------------------------------------------------------
// Rollback (delete) all extents for the specified OID, that follow the
// designated extent
//------------------------------------------------------------------------------
int rollbackExtents(OID_t oid)
{
	char      DictStoreOIDFlag;
	uint32_t  partNum;
	uint16_t  dbRoot;
	uint16_t  segNum;
	HWM_t     hwm;

	cout << "Are you rolling back extents for a dictionary store oid (y/n)? ";
	cin >> DictStoreOIDFlag;

	if ((DictStoreOIDFlag == 'y') || (DictStoreOIDFlag == 'Y'))
	{
		unsigned int     hwmCount = 0;
		vector<uint16_t> segNums;
		vector<HWM_t>    hwms;

		while (1)
		{
			cout <<"Enter DBRoot#, part#, and the number of HWMs to be entered "
				"(separated by spaces): ";
			cin >> dbRoot >> partNum >> hwmCount;
			if (isInputValid())
				break;
		}
		for (unsigned int k=0; k<hwmCount; k++)
		{
			while (1)
			{
				cout << "Enter seg# and HWM for that segment file " <<
					"(separated by spaces): ";
				cin >> segNum >> hwm;
				if (isInputValid())
					break;
			}

			hwms.push_back(hwm);
			segNums.push_back(segNum);
		}

		CHECK(emp->rollbackDictStoreExtents_DBroot( oid, dbRoot, partNum,
			segNums, hwms ));
	}
	else
	{
		while (1)
		{
			cout << "Enter DBRoot#, part#, seg#, and HWM for the last extent "
				"on that DBRoot (separated by spaces): ";
			cin >> dbRoot >> partNum >> segNum >> hwm;
			if (isInputValid())
				break;
		}

		CHECK(emp->rollbackColumnExtents_DBroot( oid, false,
			dbRoot, partNum, segNum, hwm ));
	}

	return 0;
}

//------------------------------------------------------------------------------
// Delete the specified OID from the extent map
//------------------------------------------------------------------------------
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

	CHECK(emp->deleteOID(oid));

	return 0;
}

//------------------------------------------------------------------------------
// Update the local HWM for the specified OID and segment file
//------------------------------------------------------------------------------
int editHWM(OID_t oid)
{
	HWM_t    oldHWM;
	HWM_t    newHWM;
	uint32_t partNum   = 0;
	uint16_t segNum    = 0;

	while (1)
	{
		cout << "Enter Partition#, and Segment# (separated by spaces): ";
		cin >> partNum >> segNum;
		if (isInputValid())
			break;
	}
	int extState;
	CHECK(emp->getLocalHWM(oid, partNum, segNum, oldHWM, extState));

	cout << "HWM for partition " << partNum << " and segment " << segNum <<
		" is currently " << oldHWM <<
		".  Enter new value: ";
	cin >> newHWM;

	CHECK(emp->setLocalHWM(oid, partNum, segNum, newHWM));

	return 0;
}

//------------------------------------------------------------------------------
// Dump the free list information
//------------------------------------------------------------------------------
int dumpFL()
{
	vector<InlineLBIDRange> v = emp->getEMFreeListEntries();

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

//------------------------------------------------------------------------------
// Dump information about the specified LBID
//------------------------------------------------------------------------------
int dumpLBID(LBID_t lbid)
{
	uint16_t ver = 0;
	BRM::OID_t oid;
	uint16_t dbroot;
	uint32_t partNum;
	uint16_t segNum;
	uint32_t fbo;
	int rc;
	rc = emp->lookupLocal(lbid, ver, false, oid, dbroot, partNum, segNum, fbo);
	idbassert(rc == 0);
	cout << "LBID " << lbid << " is part of OID " << oid <<
		"; DbRoot "     << dbroot  <<
		"; partition# " << partNum <<
		"; segment# "   << segNum  <<
		"; at FBO "     << fbo     << endl;
	return 0;
}

//------------------------------------------------------------------------------
// Delete all the extents for the specified DBRoot
//------------------------------------------------------------------------------
int deleteAllOnDBRoot(uint16_t dbroot)
{
	int rc;
	rc = emp->deleteDBRoot(dbroot);
	idbassert(rc == 0);
	return 0;
}

}

//------------------------------------------------------------------------------
// main entry point into this program
//------------------------------------------------------------------------------
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
	bool pflg = false;
	uint16_t dbroot = 0;
	unsigned int sortOrder = 0; // value of 0 means no sorting

	opterr = 0;

	while ((c = getopt(argc, argv, "dzCc:o:tsxue:r:fvhw:lb:aimp:S:")) != EOF)
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
        case 'u':
            uflg = true;
            break;
		case 's':
			sflg = true;
			break;
		case 'x':
			xflg = true;
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
		case 'm':
			mflg = true;
			break;
		case 'p':
			pflg = true;
			dbroot = (uint16_t)strtoul(optarg, 0, 0);
			break;
		case 'S':
			sortOrder = strtoul(optarg, 0, 0);
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

	emp = new DBRM();

    if (!emp->isDBRMReady())
    {
        cerr << endl << "Error! The DBRM is currently not responding!" << endl
            << "editem can't continue" << endl << endl;
        return 1;
    }
    MaxOID = -1;
    if (localModuleType != "um")
    {
        ObjectIDManager oidm;
        MaxOID = oidm.size();
    }

    if (emp->isReadWrite() != ERR_OK)
    {
        cerr << endl << "Warning! The DBRM is currently in Read-Only mode!" << endl
            << "Updates will not propagate!" << endl << endl;
    }

	if (iflg)
	{
		dflg = mflg = true;
		ExtentMap em;
		cout << em;
		return 0;
	}

	if ((int)dflg + (int)cflg + (int)oflg + (int)xflg + (int)eflg + (int)wflg +
		(int)lflg + (int)bflg + (int)Cflg + (int)rflg + (int)pflg > 1)
	{
		cerr << "Only one of d/c/o/x/e/w/l/b/r/C/p can be specified." << endl;
		usage(pname);
		return 1;
	}

	if ((int)sflg + (int)tflg + (int)aflg + (int)uflg > 1)
	{
		cerr << "Only one of s/t/a/u can be specified." << endl;
		usage(pname);
		return 1;
	}

	if (MaxOID < 0 && ((int)dflg + (int)Cflg + zflg) > 0)
	{
		cerr << "Can't use d/C flag on module type " << localModuleType << endl;
		usage(pname);
		return 1;
	}

	if (pflg)
		return deleteAllOnDBRoot(dbroot);

	if (oflg)
		return dumpone(oid, sortOrder);

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
		return extendOids();
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

