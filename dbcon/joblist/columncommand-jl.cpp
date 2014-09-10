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
// $Id: columncommand-jl.cpp 9655 2013-06-25 23:08:13Z xlou $
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
	//for (uint32_t i = 0; i < lastLBID.size(); i++)
	//	cout << lastLBID[i] << " ";
	//cout << endl;

// 	cout << "Init columncommand from scan with OID " << OID << endl;
	/* I think modmask isn't necessary for scans */
	divShift = scan.divShift;
	modMask = (1 << divShift) - 1;

	// @Bug 2889.  Drop partition enhancement.  Read FilesPerColumnPartition and ExtentsPerSegmentFile for use in RID calculation.
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
	fIsDict = step.fIsDict;
	ResourceManager rm;
	numDBRoots = rm.getDBRootCount();

	// grab the last LBID for this column.  It's a _minor_ optimization for the block loader.
	//dbrm.getLastLocalHWM((BRM::OID_t)OID, dbroot, partNum, segNum, lastHWM);
	//dbrm.lookupLocal((BRM::OID_t)OID, partNum, segNum, lastHWM, fLastLbid);
	/*
	fLastLbid.resize(numDBRoots);
	for (uint32_t i = 0; i < numDBRoots; i++) {
		dbrm.getLastLocalHWM2((BRM::OID_t)OID, i+1, partNum, segNum, lastHWM);
		dbrm.lookupLocal((BRM::OID_t)OID, partNum, segNum, lastHWM, fLastLbid[i]);
	}
	*/

// 	cout << "Init columncommand from step with OID " << OID << " and width " << colType.colWidth << endl;

	// @Bug 2889.  Drop partition enhancement.  Read FilesPerColumnPartition and ExtentsPerSegmentFile for use in RID calculation.
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
	for (uint32_t i = 0; i < filterString.length(); ++i)
		cout << (int) filterString.buf()[i] << " ";
	cout << endl;
#endif
	bs << (uint8_t) colType.colDataType;
	bs << (uint8_t) colType.colWidth;
	bs << (uint8_t) colType.scale;
	bs << (uint8_t) colType.compressionType;
	bs << BOP;
	bs << filterCount;
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

void ColumnCommandJL::setLBID(uint64_t rid, uint32_t dbRoot)
{
	uint32_t partNum;
	uint16_t segNum;
	uint8_t extentNum;
	uint16_t blockNum;
	uint32_t colWidth;
	uint32_t i;

	idbassert(extents.size() > 0);
	colWidth = extents[0].colWid;
	rowgroup::getLocationFromRid(rid, &partNum, &segNum, &extentNum, &blockNum);

	for (i = 0; i < extents.size(); i++) {
		if (extents[i].dbRoot == dbRoot &&
		  extents[i].partitionNum == partNum &&
		  extents[i].segmentNum == segNum &&
		  extents[i].blockOffset == (extentNum * colWidth * 1024)) {

			lbid = extents[i].range.start + (blockNum * colWidth);
            currentExtentIndex = i;
			/*
			ostringstream os;
			os << "CCJL: rid=" << rid << "; dbroot=" << dbRoot << "; partitionNum=" << partNum
				<< "; segmentNum=" << segNum <<	"; extentNum = " << (int) extentNum <<
				"; blockNum = " << blockNum << "; OID=" << OID << " LBID=" << lbid;
			cout << os.str() << endl;
			*/
			return;
		}
	}
	throw logic_error("ColumnCommandJL: setLBID didn't find the extent for the rid.");

//		ostringstream os;
//		os << "CCJL: rid=" << rid << "; dbroot=" << dbRoot << "; partitionNum=" << partitionNum << "; segmentNum=" << segmentNum << "; stripeWithinPartition=" <<
//			stripeWithinPartition << "; OID=" << OID << " LBID=" << lbid;
//		BRM::log(os.str());
}

inline uint32_t ColumnCommandJL::getFBO(uint64_t lbid)
{
	uint32_t i;
	uint64_t lastLBID;

	for (i = 0; i < extents.size(); i++) {
 		lastLBID = extents[i].range.start + (extents[i].range.size << 10) - 1;
		if (lbid >= (uint64_t) extents[i].range.start && lbid <= lastLBID)
		{

	        // @Bug 2889.  Change for drop partition.  Treat the FBO as if all of the partitions are
			// still there as one or more partitions may have been dropped. Get the original index
			// for this partition (i.e. what the index would be if all of the partitions were still
			// there).  The RIDs wind up being calculated off of this FBO for use in DML and DML
			// needs calculates the partition number, segment number, etc. off of the RID and needs
			// to remain the same when partitions are dropped.
			//
			// originalIndex = extents in partitions above +
			//		   extents in this partition in stripes above +
			//		   file offset in this stripe
			uint32_t originalIndex =
				(extents[i].partitionNum * fExtentsPerSegmentFile * fFilesPerColumnPartition) +
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
		case 16: return TableColumn::BIN16;
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
	else if (execplan::isCharType(colType.colDataType))
		ret << " (is char)";
	return ret.str();
}

uint16_t ColumnCommandJL::getWidth()
{
	return colType.colWidth;
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
