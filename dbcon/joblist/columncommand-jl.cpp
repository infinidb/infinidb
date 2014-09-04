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

//
// $Id: columncommand-jl.cpp 8829 2012-08-28 15:49:49Z pleblanc $
// C++ Implementation: columncommand
//
// Description: 
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include <string>
#include <cstdlib>
#include <sstream>
using namespace std;

#include "primitivestep.h"
#include "tablecolumn.h"
#include "bpp-jl.h"
#include "columncommand-jl.h"
#include "dbrm.h"

using namespace messageqcpp;

namespace
{
uint8_t name2ord(const string& name)
{
	uint8_t ord = 0;
/*
	if (ord == 255) return ::llabs;
	if (ord == 254) return reinterpret_cast<UDFFcnPtr_t>(::acos);
	if (ord == 253) return reinterpret_cast<UDFFcnPtr_t>(::asin);
	if (ord == 252) return reinterpret_cast<UDFFcnPtr_t>(::atan);
	if (ord == 251) return reinterpret_cast<UDFFcnPtr_t>(::cos);
	if (ord == 250) return reinterpret_cast<UDFFcnPtr_t>(cotangent);
	if (ord == 249) return reinterpret_cast<UDFFcnPtr_t>(::exp);
	if (ord == 248) return reinterpret_cast<UDFFcnPtr_t>(::log);
	if (ord == 247) return reinterpret_cast<UDFFcnPtr_t>(::log2);
	if (ord == 246) return reinterpret_cast<UDFFcnPtr_t>(::log10);
	if (ord == 245) return reinterpret_cast<UDFFcnPtr_t>(::sin);
	if (ord == 244) return reinterpret_cast<UDFFcnPtr_t>(::sqrt);
	if (ord == 243) return reinterpret_cast<UDFFcnPtr_t>(::tan);
*/
	if (name == "abs") return 255;
	if (name == "acos") return 254;
	if (name == "asin") return 253;
	if (name == "atan") return 252;
	if (name == "cos") return 251;
	if (name == "cot") return 250;
	if (name == "exp") return 249;
	if (name == "log" || name == "ln") return 248;
	if (name == "log2") return 247;
	if (name == "log10") return 246;
	if (name == "sin") return 245;
	if (name == "sqrt") return 244;
	if (name == "tan") return 243;

	string pfx(name, 0, 6);
	if (pfx != "cpfunc") return 0;
	string ordstr(name, 6);
	ord = static_cast<uint8_t>(atoi(ordstr.c_str()));
	return ord;
}
}

namespace joblist
{

ColumnCommandJL::ColumnCommandJL(const pColScanStep &scan, vector<BRM::LBID_t> lastLBID)
{
	BRM::DBRM dbrm;
	isScan = true;

	/* grab necessary vars from scan */
	traceFlags = scan.fTraceFlags;
	filterString = scan.fFilterString;
	filterCount = scan.fFilterCount;
	colType = scan.fColType;
	BOP = scan.fBOP;
	extents = scan.extents;
	OID = scan.fOid;
	colName = scan.fName;
	rpbShift = scan.rpbShift;
	fIsDict = scan.fIsDict;
	fLastLbid = lastLBID;

	//cout << "CCJL inherited lastlbids: ";
	//for (uint i = 0; i < lastLBID.size(); i++)
	//	cout << lastLBID[i] << " ";
	//cout << endl;

// 	cout << "Init columncommand from scan with OID " << OID << endl;
	/* I think modmask isn't necessary for scans */
	divShift = scan.divShift;
	modMask = (1 << divShift) - 1;
	fFcnOrd = name2ord(scan.udfName());

	// @Bug 2889.  Drop partition enhancement.  Read FilesPerColumnPartition and ExtentsPerSegmentFile for use in RID calculation.
	fExtentRows = dbrm.getExtentRows();
	fFilesPerColumnPartition = DEFAULT_FILES_PER_COLUMN_PARTITION;
	fExtentsPerSegmentFile = DEFAULT_EXTENTS_PER_SEGMENT_FILE;
	config::Config* cf = config::Config::makeConfig();
	string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
	if ( fpc.length() != 0 )
		fFilesPerColumnPartition = cf->uFromText(fpc);
	string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
	if ( epsf.length() != 0 )
		fExtentsPerSegmentFile = cf->uFromText(epsf);
}

ColumnCommandJL::ColumnCommandJL(const pColScanStep &scan)
{
	BRM::DBRM dbrm;
	uint32_t partNum;
	uint16_t segNum;
	BRM::HWM_t lastHWM;
	int err;
	isScan = true;

	/* grab necessary vars from scan */
	traceFlags = scan.fTraceFlags;
	filterString = scan.fFilterString;
	filterCount = scan.fFilterCount;
	colType = scan.fColType;
	BOP = scan.fBOP;
	extents = scan.extents;
	OID = scan.fOid;
	colName = scan.fName;
	rpbShift = scan.rpbShift;
	fIsDict = scan.fIsDict;

// 	cout << "Init columncommand from scan with OID " << OID << endl;
	/* I think modmask isn't necessary for scans */
	divShift = scan.divShift;
	modMask = (1 << divShift) - 1;
	fFcnOrd = name2ord(scan.udfName());

	// PL - each dbroot will have a different HWM now; need to try to avoid needing this.
	//get last lbid of this oid
	ResourceManager rm;
	numDBRoots = rm.getDBRootCount();

	fLastLbid.resize(numDBRoots);
	for (uint i = 0; i < numDBRoots; i++) {
		err = dbrm.getLastHWM_DBroot((BRM::OID_t)OID, i+1, partNum, segNum, lastHWM);
		if (err != 0)
			dbrm.lookupLocal_DBroot((BRM::OID_t)OID, i+1, partNum, segNum, lastHWM, fLastLbid[i]);
		else
			fLastLbid[i] = 0;
	}
	//cout << "CCJL calculated lastlbids=";
	//for (uint i = 0; i < fLastLbid.size(); i++)
	//	cout << fLastLbid[i] << " ";
	//cout << endl;

	// @Bug 2889.  Drop partition enhancement.  Read FilesPerColumnPartition and ExtentsPerSegmentFile for use in RID calculation.
	fExtentRows = dbrm.getExtentRows();
	fFilesPerColumnPartition = DEFAULT_FILES_PER_COLUMN_PARTITION;
	fExtentsPerSegmentFile = DEFAULT_EXTENTS_PER_SEGMENT_FILE;
	config::Config* cf = config::Config::makeConfig();
	string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
	if ( fpc.length() != 0 )
		fFilesPerColumnPartition = cf->uFromText(fpc);
	string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
	if ( epsf.length() != 0 )
		fExtentsPerSegmentFile = cf->uFromText(epsf);
}

ColumnCommandJL::ColumnCommandJL(const pColStep &step)
{
	BRM::DBRM dbrm;

	isScan = false;

	/* grab necessary vars from step */
	traceFlags = step.fTraceFlags;
	filterString = step.fFilterString;
	filterCount = step.fFilterCount;
	colType = step.fColType;
	BOP = step.fBOP;
	extents = step.extents;
	divShift = step.divShift;
	modMask = step.modMask;
	rpbShift = step.rpbShift;
	OID = step.fOid;
	colName = step.fName;
	fFcnOrd = name2ord(step.udfName());
	fIsDict = step.fIsDict;
	ResourceManager rm;
	numDBRoots = rm.getDBRootCount();
	
	// grab the last LBID for this column.  It's a _minor_ optimization for the block loader.
	//dbrm.getLastLocalHWM((BRM::OID_t)OID, dbroot, partNum, segNum, lastHWM);
	//dbrm.lookupLocal((BRM::OID_t)OID, partNum, segNum, lastHWM, fLastLbid);
	/*
	fLastLbid.resize(numDBRoots);
	for (uint i = 0; i < numDBRoots; i++) {
		dbrm.getLastLocalHWM2((BRM::OID_t)OID, i+1, partNum, segNum, lastHWM);
		dbrm.lookupLocal((BRM::OID_t)OID, partNum, segNum, lastHWM, fLastLbid[i]);
	}
	*/

// 	cout << "Init columncommand from step with OID " << OID << " and width " << colType.colWidth << endl;

	// @Bug 2889.  Drop partition enhancement.  Read FilesPerColumnPartition and ExtentsPerSegmentFile for use in RID calculation.
	fExtentRows = dbrm.getExtentRows();
	fFilesPerColumnPartition = DEFAULT_FILES_PER_COLUMN_PARTITION;
	fExtentsPerSegmentFile = DEFAULT_EXTENTS_PER_SEGMENT_FILE;
	config::Config* cf = config::Config::makeConfig();
	string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
	if ( fpc.length() != 0 )
		fFilesPerColumnPartition = cf->uFromText(fpc);
	string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
	if ( epsf.length() != 0 )
		fExtentsPerSegmentFile = cf->uFromText(epsf);
}

ColumnCommandJL::~ColumnCommandJL()
{
}

void ColumnCommandJL::createCommand(ByteStream &bs) const
{
	bs << (uint8_t) COLUMN_COMMAND;
	bs << (uint8_t) isScan;
// 	cout << "sending lbid " << lbid << endl;
	bs << traceFlags;
	bs << filterString;
#if 0
	cout << "filter string: ";
	for (uint i = 0; i < filterString.length(); ++i)
		cout << (int) filterString.buf()[i] << " ";
	cout << endl;
#endif
	bs << (uint8_t) colType.colDataType;
	bs << (uint8_t) colType.colWidth;
	bs << (uint8_t) colType.scale;
	bs << (uint8_t) colType.compressionType;
	bs << BOP;
	bs << filterCount;
	bs << fFcnOrd;
	serializeInlineVector(bs, fLastLbid);
	//bs << (uint64_t)fLastLbid;

	CommandJL::createCommand(bs);

	/* XXXPAT: for debugging only, we can get rid of this one */
// 	bs << colType.columnOID;

// 	cout << "filter Count is " << filterCount << " ----" << endl;
}

void ColumnCommandJL::runCommand(ByteStream &bs) const
{
	bs << lbid;
}

void ColumnCommandJL::setLBID(uint64_t rid, uint dbRoot)
{
	uint64_t l_rid, fbo;

	if (isScan) {
		lbid = rid;   	//it's not a rid here need to convert it to a rid
		fbo = getFBO(lbid);
		l_rid = fbo << rpbShift;
		bpp->setLBIDForScan(l_rid, dbRoot);
	}
	else {
		uint extentIndex = 0;
		uint extentOffset = 0;
		fbo = (rid >> 13);	// a logical block FBO
		fbo <<= (13-rpbShift);   // convert to a real FBO rounding down to a logical block boundary
		extentOffset = fbo & modMask;

		// @Bug 2889.  Changed logic to find the extentIndex for the drop partition enhancement.  The passed in RID is 
		// now relative to partition 0 and one or more partitions may have been dropped.  Find the extent in the extents
		// array rather than doing the math directly off of the fbo. 
		// extentIndex = fbo >> divShift;

		// partitionNum = "RID / rows per partition"
		uint64_t partitionNum = rid / ( fFilesPerColumnPartition * fExtentsPerSegmentFile * fExtentRows );

		// segmentNum = (RID % rows per partition) / rows per extent) % files per partition
		//            = (extent # within its partition) % files per partition
		uint64_t segmentNum =
			(((rid % (fFilesPerColumnPartition * fExtentsPerSegmentFile * fExtentRows)) / fExtentRows))
			% fFilesPerColumnPartition;

		// stripeWithinPartition = (RID - rows up to the partition before this one) / ??
		uint64_t stripeWithinPartition = 
			(rid - (partitionNum * fExtentsPerSegmentFile * fFilesPerColumnPartition * fExtentRows)) /
                        (fFilesPerColumnPartition * fExtentRows);
		uint64_t blockOffset = stripeWithinPartition * colType.colWidth * 1024;
		
		bool found = false;

		for(uint i = 0; i < extents.size(); i++)
		{
			if(extents[i].dbRoot == dbRoot && (extents[i].partitionNum == partitionNum) &&
					(extents[i].segmentNum == segmentNum) && (extents[i].blockOffset == blockOffset))
			{
				found = true;
				extentIndex = i;	
			}
		}
		if(!found)
		{
			 throw logic_error("ColumnCommandJL: setLBID didn't find the extent for the rid.");
		}

		// Calculate the lbid.
		lbid = extents[extentIndex].range.start + extentOffset;
//		ostringstream os;
//		os << "CCJL: rid=" << rid << "; dbroot=" << dbRoot << "; partitionNum=" << partitionNum << "; segmentNum=" << segmentNum << "; stripeWithinPartition=" <<
//			stripeWithinPartition << "; OID=" << OID << " LBID=" << lbid;
//		BRM::log(os.str());
	}
}

inline uint32_t ColumnCommandJL::getFBO(uint64_t lbid)
{
	uint i;
	uint64_t lastLBID;

	for (i = 0; i < extents.size(); i++) {
 		lastLBID = extents[i].range.start + (extents[i].range.size << 10) - 1;
		if (lbid >= (uint64_t) extents[i].range.start && lbid <= lastLBID)
		{

	        // @Bug 2889.  Change for drop partition.  Treat the FBO as if all of the partitions are still there as one or more partitions
			// may have been dropped. Get the original index for this partition (i.e. what the index would be if all of the partitions 
			// were still there).  The RIDs wind up being calculated off of this FBO for use in DML and DML needs calculates the partition
			// number, segment number, etc. off of the RID and needs to remain the same when partitions are dropped.

			// originalIndex = extents in partitions above +
			//		   extents in this partition in stripes above +
			//		   file offset in this stripe
			uint originalIndex = (extents[i].partitionNum * fExtentsPerSegmentFile * fFilesPerColumnPartition) + 
				((extents[i].blockOffset / extents[i].colWid / 1024) * fFilesPerColumnPartition) + 	      
				extents[i].segmentNum;

			return (lbid - extents[i].range.start) + (originalIndex << divShift);
		}
	}
	throw logic_error("ColumnCommandJL: didn't find the FBO?");
}

uint8_t ColumnCommandJL::getTableColumnType()
{
	switch (colType.colWidth) {
		case 8: return TableColumn::UINT64;
		case 4: return TableColumn::UINT32;
		case 2: return TableColumn::UINT16;
		case 1: return TableColumn::UINT8;
		default:
			throw logic_error("ColumnCommandJL: bad column width");
	}
}

string ColumnCommandJL::toString()
{
	ostringstream ret;

	ret << "ColumnCommandJL: " << filterCount << " filters  colwidth=" << 
		colType.colWidth << " oid=" << OID << " name=" << colName;
	if (isScan)
		ret << " (scan)";
	if (isDict())
		ret << " (tokens)";
	else if (isCharType())
		ret << " (is char)";
	return ret.str();
}

uint16_t ColumnCommandJL::getWidth()
{
	return colType.colWidth;
}

bool ColumnCommandJL::isCharType() const
{
	return (colType.colDataType == execplan::CalpontSystemCatalog::CHAR ||
	  colType.colDataType == execplan::CalpontSystemCatalog::VARCHAR);
}

void ColumnCommandJL::reloadExtents()
{
	int err;
	BRM::DBRM dbrm;

	err = dbrm.getExtents(OID, extents);
	if (err) {
		ostringstream os;
		os << "pColStep: BRM lookup error. Could not get extents for OID " << OID;
		throw runtime_error(os.str());
	}

	sort(extents.begin(), extents.end(), BRM::ExtentSorter());
}






};
