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
// $Id: columncommand.h 2057 2013-02-13 17:00:10Z pleblanc $
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

#ifndef COLUMNCOMMAND_H_
#define COLUMNCOMMAND_H_

#include "command.h"
#include "calpontsystemcatalog.h"

namespace primitiveprocessor
{

// #warning got the ColumnCommand definition
class ColumnCommand : public Command
{
public:
	ColumnCommand();
	virtual ~ColumnCommand();

	inline uint64_t getLBID() { return lbid; }
	inline uint8_t getWidth() { return colType.colWidth; }
	inline uint8_t getScale() { return colType.scale; }
	uint16_t getFilterCount() { return filterCount; }
	const execplan::CalpontSystemCatalog::ColType& getColType() { return colType; }

	void execute();
	void execute(int64_t *vals);	//used by RTSCommand to redirect values
	void prep(int8_t outputType, bool makeAbsRids);
	void project();
	void projectIntoRowGroup(rowgroup::RowGroup &rg, uint32_t pos);
	void nextLBID();
	bool isScan() { return _isScan; }
	void createCommand(messageqcpp::ByteStream &);
	void resetCommand(messageqcpp::ByteStream &);
	void setMakeAbsRids(bool m) { makeAbsRids = m; }
	bool willPrefetch();
	const uint64_t getEmptyRowValue( const execplan::CalpontSystemCatalog::ColDataType dataType, const int width ) const;
	const int64_t getLastLbid();
	void getLBIDList(uint32_t loopCount, std::vector<int64_t> *lbids);

	virtual SCommand duplicate();
	bool operator==(const ColumnCommand &) const;
	bool operator!=(const ColumnCommand &) const;

	/* OR hacks */
	void setScan(bool b) { _isScan = b; }
	void disableFilters();
	void enableFilters();

	int getCompType() const { return colType.compressionType; }

protected:
	virtual void loadData();
	void duplicate(ColumnCommand *);

	// we only care about the width and type fields.
	//On the PM the rest is uninitialized
	execplan::CalpontSystemCatalog::ColType colType;

private:
	ColumnCommand(const ColumnCommand &);
	ColumnCommand& operator=(const ColumnCommand &);

	void _execute();
	void issuePrimitive();
	void processResult();
	void process_OT_BOTH();
	void process_OT_RID();
	void process_OT_DATAVALUE();
	void process_OT_ROWGROUP();
	void projectResult();
	void projectResultRG(rowgroup::RowGroup &rg, uint32_t pos);
	void removeRowsFromRowGroup(rowgroup::RowGroup &);
	void makeScanMsg();
	void makeStepMsg();
	void setLBID(uint64_t rid);

	bool _isScan;

	boost::scoped_array<uint8_t> inputMsg;
	NewColRequestHeader *primMsg;
	NewColResultHeader *outMsg;

	// the length of base prim msg, which is everything up to the
	// rid array for the pCol message
	uint32_t baseMsgLength;

	uint64_t lbid;
	uint32_t traceFlags;  // probably move this to Command
	uint8_t BOP;
	messageqcpp::ByteStream filterString;
	uint16_t filterCount;
	bool makeAbsRids;
	int64_t *values;      // this is usually bpp->values; RTSCommand needs to use a different container

	uint8_t mask, shift;  // vars for the selective block loader

	// counters to decide whether to prefetch or not
	uint32_t blockCount, loadCount;

	boost::shared_ptr<primitives::ParsedColumnFilter> parsedColumnFilter;

	/* OR hacks */
	boost::shared_ptr<primitives::ParsedColumnFilter> emptyFilter;
	bool suppressFilter;

	std::vector<uint64_t> lastLbid;

	/* speculative optimizations for projectintorowgroup() */
	rowgroup::Row r;
	uint32_t rowSize;

	bool wasVersioned;

	friend class RTSCommand;
};

}

#endif
// vim:ts=4 sw=4:

