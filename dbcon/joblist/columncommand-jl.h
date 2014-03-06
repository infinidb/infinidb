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
// $Id: columncommand-jl.h 9655 2013-06-25 23:08:13Z xlou $
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

#include "primitivestep.h"
#include "command-jl.h"

namespace joblist
{

class ColumnCommandJL : public CommandJL
{
public:
	ColumnCommandJL(const pColScanStep &, std::vector<BRM::LBID_t> lastLBID);
	ColumnCommandJL(const pColStep &);
	virtual ~ColumnCommandJL();

	virtual void createCommand(messageqcpp::ByteStream &bs) const;
	virtual void runCommand(messageqcpp::ByteStream &bs) const;
	void setLBID(uint64_t rid, uint32_t dbroot);
	uint8_t getTableColumnType();
	virtual std::string toString();
	uint16_t getWidth();
	CommandType getCommandType() { return COLUMN_COMMAND; }
	// @bug 1098
	const uint8_t getBOP() const { return BOP; }
	const messageqcpp::ByteStream& getFilterString() { return filterString; }
	const uint16_t getFilterCount() const { return filterCount; }
	const std::vector<struct BRM::EMEntry>& getExtents() { return extents; }
	const execplan::CalpontSystemCatalog::ColType& getColType() const { return colType; }
	bool isDict() const { return fIsDict; }

	void  scan(bool b) { isScan = b; }
	bool  scan() { return isScan; }

	void reloadExtents();

protected:
    uint32_t currentExtentIndex;
	messageqcpp::ByteStream filterString;
	std::vector<struct BRM::EMEntry> extents;
	execplan::CalpontSystemCatalog::ColType colType;

private:
	ColumnCommandJL();
	ColumnCommandJL(const ColumnCommandJL &);

	uint32_t getFBO(uint64_t lbid);

	bool isScan;
	uint64_t lbid;
	uint32_t traceFlags;  // probably move this to Command
	uint8_t BOP;
	uint32_t rpbShift, divShift, modMask;
	uint16_t filterCount;
	std::vector<BRM::LBID_t> fLastLbid;

	bool fIsDict;

	// @Bug 2889.  Added two members below for drop partition enhancement.
	// RJD: make sure that we keep enough significant digits around for partition math
	uint64_t fFilesPerColumnPartition;
	uint64_t fExtentsPerSegmentFile;

	uint32_t numDBRoots;
	uint32_t dbroot;

	static const unsigned DEFAULT_FILES_PER_COLUMN_PARTITION = 32;
	static const unsigned DEFAULT_EXTENTS_PER_SEGMENT_FILE   =  4;
};

}

#endif
// vim:ts=4 sw=4:

