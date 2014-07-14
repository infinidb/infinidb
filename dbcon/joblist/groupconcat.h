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

//  $Id: groupconcat.h 9705 2013-07-17 20:06:07Z pleblanc $


/** @file */

#ifndef GROUP_CONCAT_H
#define GROUP_CONCAT_H

#include <utility>
#include <set>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_array.hpp>

#include "returnedcolumn.h" // SRCP
#include "rowgroup.h"       // RowGroup
#include "rowaggregation.h" // SP_GroupConcat
#include "limitedorderby.h" // IdbOrderBy

#if defined(_MSC_VER) && defined(JOBLIST_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace joblist
{

// forward reference
struct JobInfo;
class  GroupConcator;
class  ResourceManager;


class GroupConcatInfo
{
public:
	GroupConcatInfo();
	virtual ~GroupConcatInfo();

	void prepGroupConcat(JobInfo&);
	void mapColumns(const rowgroup::RowGroup&);

	std::set<uint32_t>& columns() { return fColumns; }
	std::vector<rowgroup::SP_GroupConcat>& groupConcat() { return fGroupConcat; }

	const std::string toString() const;

protected:
	uint32_t getColumnKey(const execplan::SRCP& srcp, JobInfo& jobInfo);
	boost::shared_array<int> makeMapping(const rowgroup::RowGroup&, const rowgroup::RowGroup&);

	std::set<uint32_t>                        fColumns;
	std::vector<rowgroup::SP_GroupConcat> fGroupConcat;
};


class GroupConcatAgUM : public rowgroup::GroupConcatAg
{
public:
	EXPORT GroupConcatAgUM(rowgroup::SP_GroupConcat&);
	EXPORT ~GroupConcatAgUM();

	void initialize();
	void processRow(const rowgroup::Row&);
	EXPORT void merge(const rowgroup::Row&, int64_t);
	boost::scoped_ptr<GroupConcator>& concator() { return fConcator; }

	EXPORT void getResult(uint8_t*);
	EXPORT uint8_t * getResult();

protected:
	void applyMapping(const boost::shared_array<int>&, const rowgroup::Row&);

	boost::scoped_ptr<GroupConcator>      fConcator;
	boost::scoped_array<uint8_t>          fData;
	rowgroup::Row                         fRow;
	bool                                  fNoOrder;
};


// GROUP_CONCAT base
class GroupConcator
{
public:
	GroupConcator();
	virtual ~GroupConcator();

	virtual void initialize(const rowgroup::SP_GroupConcat&);
	virtual void processRow(const rowgroup::Row&) = 0;

	virtual void merge(GroupConcator*) = 0;
	virtual void getResult(uint8_t* buff, const std::string &sep) = 0;
	virtual uint8_t * getResult(const std::string &sep);

	virtual const std::string toString() const;

protected:
	virtual bool concatColIsNull(const rowgroup::Row&);
	virtual void outputRow(std::ostringstream&, const rowgroup::Row&);
	virtual int64_t lengthEstimate(const rowgroup::Row&);

	std::vector<uint32_t>                 fConcatColumns;
	std::vector<std::pair<std::string, uint32_t> >  fConstCols;
	int64_t                               fCurrentLength;
	int64_t                               fGroupConcatLen;
	int64_t                               fConstantLen;
	boost::scoped_array<uint8_t>          fOutputString;
};


// For GROUP_CONCAT withour distinct or orderby
class GroupConcatNoOrder : public GroupConcator
{
public:
	GroupConcatNoOrder();
	virtual ~GroupConcatNoOrder();

	void initialize(const rowgroup::SP_GroupConcat&);
	void processRow(const rowgroup::Row&);

	void merge(GroupConcator*);
	void getResult(uint8_t* buff, const std::string &sep);

	const std::string toString() const;

protected:
	rowgroup::RowGroup                    fRowGroup;
	rowgroup::Row                         fRow;
	rowgroup::RGData                      fData;
	std::queue<rowgroup::RGData>          fDataQueue;
	uint64_t                              fRowsPerRG;
	uint64_t                              fErrorCode;
	uint64_t                              fMemSize;
	ResourceManager*                      fRm;
	boost::shared_ptr<int64_t>			  fSessionMemLimit;
};


// ORDER BY used in GROUP_CONCAT class
// This version is for GROUP_CONCAT, the size is limited by the group_concat_max_len.
class GroupConcatOrderBy : public GroupConcator, public ordering::IdbOrderBy
{
public:
	GroupConcatOrderBy();
	virtual ~GroupConcatOrderBy();

	void initialize(const rowgroup::SP_GroupConcat&);
	void processRow(const rowgroup::Row&);
	uint64_t getKeyLength() const;

	void merge(GroupConcator*);
	void getResult(uint8_t* buff, const std::string &sep);

	const std::string toString() const;

protected:
};

}

#undef EXPORT

#endif  // GROUP_CONCAT_H

