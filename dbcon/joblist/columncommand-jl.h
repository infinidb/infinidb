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
// $Id: columncommand-jl.h 8765 2012-07-30 17:29:49Z pleblanc $
// C++ Interface: columncommand
//
// Description: 
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//
/** @file */

#ifndef COLUMNCOMMANDJL_H_
#define COLUMNCOMMANDJL_H_

#include "jobstep.h"
#include "command-jl.h"

namespace joblist
{

class ColumnCommandJL : public CommandJL
{
public:
	ColumnCommandJL(const pColScanStep &, BRM::LBID_t lastLBID);
	ColumnCommandJL(const pColScanStep &);
	ColumnCommandJL(const pColStep &);
	virtual ~ColumnCommandJL();

	void createCommand(messageqcpp::ByteStream &bs) const;
	void runCommand(messageqcpp::ByteStream &bs) const;
	void setLBID(uint64_t rid);
	uint8_t getTableColumnType();
	std::string toString();
	uint16_t getWidth();
	CommandType getCommandType() { return COLUMN_COMMAND; }
	// @bug 1098
	const uint8_t getBOP() const { return BOP; }
	const messageqcpp::ByteStream& getFilterString() { return filterString; }
	const uint16_t getFilterCount() const { return filterCount; }
	std::vector<struct BRM::EMEntry>& getExtents() { return extents; }
	const execplan::CalpontSystemCatalog::ColType& getColType() const { return colType; }
	bool isCharType() const;
	bool isDict() const { return fIsDict; }

	void  scan(bool b) { isScan = b; }
	bool  scan() { return isScan; }
	uint8_t fcnOrd() { return fFcnOrd; }

	void reloadExtents();

private:
	ColumnCommandJL();
	ColumnCommandJL(const ColumnCommandJL &);

	uint32_t getFBO(uint64_t lbid);

	bool isScan;
	uint64_t lbid;
	uint32_t traceFlags;  // probably move this to Command
	uint8_t BOP;
	uint32_t rpbShift, divShift, modMask;
	std::vector<struct BRM::EMEntry> extents;
	messageqcpp::ByteStream filterString;
	uint16_t filterCount;
	uint8_t fFcnOrd;
	BRM::LBID_t fLastLbid;

	// care about colDataType, colWidth and scale fields.  On the PM the rest is uninitialized
	execplan::CalpontSystemCatalog::ColType colType;
	bool fIsDict;

	// @Bug 2889.  Added two members below for drop partition enhancement.
	// RJD: make sure that we keep enough significant digits around for partition math
	uint64_t fFilesPerColumnPartition;
	uint64_t fExtentsPerSegmentFile;
	uint64_t fExtentRows;
	static const unsigned DEFAULT_FILES_PER_COLUMN_PARTITION = 32;
	static const unsigned DEFAULT_EXTENTS_PER_SEGMENT_FILE   =  4;
};

}

#endif
// vim:ts=4 sw=4:

