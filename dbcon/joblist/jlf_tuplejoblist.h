/* Copyright (C) 2013 Calpont Corp.

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

//  $Id: jlf_tuplejoblist.h 9655 2013-06-25 23:08:13Z xlou $


/** @file */

#ifndef JLF_TUPLEJOBLIST_H
#define JLF_TUPLEJOBLIST_H

#include "calpontsystemcatalog.h"
#include "joblist.h"
#include "jlf_common.h"

namespace joblist
{

// data to construct and config a tuple hashjoin [used by small side table]
struct JoinInfo
{
	execplan::CalpontSystemCatalog::OID fTableOid;
	std::string               fAlias;
	std::string               fSchema;
	std::string               fView;
	AnyDataListSPtr           fDl;       // output data list
	rowgroup::RowGroup        fRowGroup; // rowgroup meta data for the data list
	// colOid and alias can be retrieved from JobInfo.tupleKeyVec using join key.

	// @bug 1495 compound join
	JoinData         fJoinData;

	JoinInfo() : fTableOid(-1) {}
};
typedef boost::shared_ptr<JoinInfo> SP_JoinInfo;


// data to construct and config a token hashjoin for string access predicates
struct DictionaryScanInfo
{
	uint                      fTokenId;  // token unique id
	AnyDataListSPtr           fDl;       // data list
	rowgroup::RowGroup        fRowGroup; // rowgroup meta data for the data list

	DictionaryScanInfo() : fTokenId(-1) { }
};


// steps and joins of a table in a query
struct TableInfo
{
	execplan::CalpontSystemCatalog::OID fTableOid;
	std::string               fName;
	std::string               fAlias;
	std::string               fSchema;
	std::string               fView;
	uint64_t                  fSubId;
	JobStepVector             fQuerySteps;
	JobStepVector             fProjectSteps;
	JobStepVector             fOneTableExpSteps;
	std::vector<uint>         fProjectCols;
	std::vector<uint>         fColsInExp1;    // 1 table expression
	std::vector<uint>         fColsInExp2;    // 2 or more tables in expression
	std::vector<uint>         fColsInRetExp;  // expression in selection/group by clause
	std::vector<uint>         fColsInOuter;   // delayed outer join filter
	std::vector<uint>         fColsInColMap;  // columns in column map
	std::vector<uint>         fJoinKeys;
	std::vector<uint>         fAdjacentList;  // tables joined with
	bool                      fVisited;

	AnyDataListSPtr           fDl;            // output data list
	rowgroup::RowGroup        fRowGroup;      // output rowgroup meta data
	std::set<uint>            fJoinedTables;  // tables directly/indirectly joined to this table

	TableInfo() : fTableOid(-1), fVisited(false) {}
};
typedef std::map<uint32_t, TableInfo> TableInfoMap;

// combine and associate tuple job steps
void associateTupleJobSteps(JobStepVector& querySteps,
							JobStepVector& projectSteps,
							DeliveredTableMap& deliverySteps,
							JobInfo& jobInfo,
							const bool overrideLargeSideEstimate);


// make BOP_OR an expression step
void orExpresssion(const execplan::Operator* op, JobInfo& jobInfo);

// union the queries and return the tuple union step
SJSTEP unionQueries(JobStepVector& queries,
							uint64_t distinctUnionNum,
							JobInfo& jobInfo);

}

#endif  // JLF_TUPLEJOBLIST_H

